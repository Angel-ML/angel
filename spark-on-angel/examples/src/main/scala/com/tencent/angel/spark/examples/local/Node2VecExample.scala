package com.tencent.angel.spark.examples.local

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.embedding.node2vec.Node2Vec
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Node2VecExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "data/graph/karate_club_network.txt")
    val output = params.getOrElse("output", "model/node2vec")
    val batchSize = params.getOrElse("batchSize", "128").toInt
    val psPartNum = params.getOrElse("psPartNum", "1").toInt
    val dataPartNum = params.getOrElse("dataPartNum", "1").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val delimiter = params.getOrElse("delimiter", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val mode = params.getOrElse("mode", "local")
    val isWeighted = params.getOrElse("isWeighted", "true").toBoolean
    val walkLength = params.getOrElse("walkLength", "5").toInt
    val pValue = params.getOrElse("pValue", "0.8").toDouble
    val qValue = params.getOrElse("qValue", "1.2").toDouble
    val needReplicaEdge = params.getOrElse("needReplicaEdge", "true").toBoolean
    val useTrunc = params.getOrElse("useTrunc", "false").toBoolean
    val truncLength = params.getOrElse("truncLength", "6000").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val setCheckPoint = params.getOrElse("setCheckPoint", "false").toBoolean
    val epochNum = params.getOrElse("epochNum", "1").toInt

    // Spark setup
    val spark = SparkSession.builder()
      .master(mode)
      .appName("node2vec")
      .getOrCreate()

    // PS setup
    val start = System.currentTimeMillis()

    val data = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex,
      sep = delimiter)
    data.printSchema()
    println("the data loading time: " + ((System.currentTimeMillis()-start)/1000.0))

    val n2v = new Node2Vec()
      .setPSPartitionNum(psPartNum)
      .setPartitionNum(dataPartNum)
      .setBatchSize(batchSize)
      .setEpochNum(epochNum)
      .setWalkLength(walkLength)
      .setPValue(pValue)
      .setQValue(qValue)
      .setNeedReplicaEdge(needReplicaEdge)
      .setIsTrunc(useTrunc)
      .setIsWeighted(isWeighted)
      .setTruncLength(truncLength)
      .setUseBalancePartition(useBalancePartition)
      .setStorageLevel(storageLevel)
      .setCheckPoint(setCheckPoint)

    n2v.setOutputDir(output)
    // sampling walkpaths
    println("begin to fit|train ...")
    if (!isWeighted) {
      n2v.transform(data)
    } else {
      n2v.transformWithWeights(data)
    }
    println("fit|train finished!")
    val end = System.currentTimeMillis()
    println(s"the elapsed time: ${1.0 * (end - start) / 1000}")

    println("Stop PS ...")
    Node2Vec.stopPS()
    println("PS Stopped!")

    println("Stop Spark ...")
    spark.stop()
    println("Spark Stopped!")
  }

}
