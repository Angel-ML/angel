package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.louvain.Louvain.edgeTripleRDD2GraphPartitions
import com.tencent.angel.spark.ml.graph.louvain.{Louvain, LouvainGraphPartition}
import com.tencent.angel.spark.ml.louvain.{Louvain, LouvainGraphPartition, LouvainPSModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LouvainExample {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val numFold = params.getOrElse("numFold", "3").toInt
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val numOpt = params.getOrElse("numOpt", "4").toInt
    val output = params.getOrElse("output", null)
    val enableCheck = params.getOrElse("enableCheck", "false").toBoolean
    val eps = params.getOrElse("eps", "0.0").toDouble

    start(mode)
    val edges = SparkContext.getOrCreate().textFile(input, partitionNum).flatMap { line =>
      val arr = line.split("[\\s+,]")
      val src = arr(0).toInt
      val dst = arr(1).toInt
      if (src < dst) {
        Iterator(((src, dst), 1.0f))
      } else if (dst < src) {
        Iterator(((dst, src), 1.0f))
      } else {
        Iterator.empty
      }
    }.reduceByKey(_ + _, partitionNum).map { case ((src, dst), wgt) => (src, dst, wgt) }
    val graph: RDD[LouvainGraphPartition] = edgeTripleRDD2GraphPartitions(edges, storageLevel = storageLevel)
    val maxId = graph.map(_.maxIdInPart).fold(Int.MinValue)(math.max)
    val model = LouvainPSModel.apply(maxId + 1)
    var louvain = new Louvain(graph, model)
    louvain.updateNodeWeightsToPS()
    louvain.modularityOptimize(numOpt, batchSize, eps)

    // correctIds
    var totalSum = louvain.checkTotalSum(model)
    louvain.correctCommunityId(model)

    if (enableCheck) {
      assert(louvain.checkCommId(model) == 0)
      assert(louvain.checkTotalSum(model) == totalSum)
    }


    var foldIter = 0
    while (foldIter < numFold) {
      foldIter += 1
      louvain = louvain.folding(batchSize, storageLevel)
      louvain.modularityOptimize(numOpt, batchSize, eps)

      // correctIds
      totalSum = louvain.checkTotalSum(model)
      louvain.correctCommunityId(model)
      if (foldIter < numFold && enableCheck) {
        assert(louvain.checkCommId(model) == 0)
        assert(louvain.checkTotalSum(model) == totalSum)
      }
    }
    // save
    louvain.save(output)
    stop()
  }

  def start(mode: String): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("louvain")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
