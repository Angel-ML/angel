package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.embedding.Param
import com.tencent.angel.spark.ml.embedding.line2.LINEModel
import com.tencent.angel.spark.ml.feature.{Features, SubSampling}
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object LINEExample2 {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    val conf = new SparkConf().setMaster("yarn-cluster").setAppName("LINE")
    val sc = new SparkContext(conf)

    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)
    conf.set(AngelConf.ANGEL_PS_BACKUP_MATRICES, "")

    val executorJvmOptions = " -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "
    println(s"executorJvmOptions = ${executorJvmOptions}")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)

    PSContext.getOrCreate(sc)

    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", "")
    val embeddingDim = params.getOrElse("embedding", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val numEpoch = params.getOrElse("epoch", "10").toInt

    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val numPartitions = params.getOrElse("numParts", "10").toInt
    val withSubSample = params.getOrElse("subSample", "true").toBoolean
    val withRemapping = params.getOrElse("remapping", "true").toBoolean
    val order = params.get("order").fold(2)(_.toInt)
    val checkpointInterval = params.getOrElse("interval", "10").toInt


    val numCores = SparkUtils.getNumCores(conf)

    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    val numDataPartitions = (numCores * 6.25).toInt
    println(s"numDataPartitions=$numDataPartitions")

    val data = sc.textFile(input)
    data.persist(StorageLevel.DISK_ONLY)

    var corpus: RDD[Array[Int]] = null

    if (withRemapping) {
      val temp = Features.corpusStringToInt(data)
      corpus = temp._1
      temp._2.map(f => s"${f._1}:${f._2}").saveAsTextFile(output + "/mapping")
    } else {
      corpus = Features.corpusStringToIntWithoutRemapping(data)
    }

    val(maxNodeId, docs) = if (withSubSample) {
      corpus.persist(StorageLevel.DISK_ONLY)
      val subsampleTmp = SubSampling.sampling(corpus)
      (subsampleTmp._1, subsampleTmp._2.repartition(numDataPartitions))
    } else {
      val tmp = corpus.repartition(numDataPartitions)
      (tmp.map(_.max).max().toLong + 1, tmp)
    }
    val edges = docs.map{
      arr =>
        (arr(0), arr(1))
    }

    edges.persist(StorageLevel.DISK_ONLY)

    val numEdge = edges.count()
    println(s"numEdge=$numEdge maxNodeId=$maxNodeId")

    corpus.unpersist()
    data.unpersist()

    val param = new Param()
      .setLearningRate(stepSize)
      .setEmbeddingDim(embeddingDim)
      .setBatchSize(batchSize)
      .setSeed(Random.nextInt())
      .setNumPSPart(Some(numPartitions))
      .setNumEpoch(numEpoch)
      .setNegSample(numNegSamples)
      .setMaxIndex(maxNodeId)
      .setNumRowDataSet(numEdge)
      .setOrder(order)
      .setModelCPInterval(checkpointInterval)

    val model = new LINEModel(param)
    model.train(edges, param, output)
    model.save(output, numEpoch)

    PSContext.stop()
    sc.stop()
  }
}
