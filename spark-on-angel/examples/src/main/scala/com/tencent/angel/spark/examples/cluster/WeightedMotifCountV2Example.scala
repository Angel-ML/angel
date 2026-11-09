package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.statistics.triangle.WeightedMotifCountingV2
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object WeightedMotifCountV2Example {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode","yarn-cluster")
    val input = params.getOrElse("input", null)
    val userGroupInput = params.getOrElse("userGroupInput", null)
    val output = params.getOrElse("output", null)

    val sep = params.getOrElse("sep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "1").toInt

    val batchSize = params.getOrElse("batchSize", "500").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "500").toInt

    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val enableCheck = params.getOrElse("enableCheck", "false").toBoolean
    val bufferSize = params.getOrElse("bufferSize", "1000000").toInt

    // for hdfs input, the format is "src dst edgeType weight"
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "3").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val hasEdgeType = params.getOrElse("hasEdgeType", "false").toBoolean
    val edgeTypeIndex = params.getOrElse("edgeTypeIndex", "2").toInt

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    val sc = start(mode)
    sc.setCheckpointDir(cpDir)

    val startTime = System.currentTimeMillis()
    val motifCount = new WeightedMotifCountingV2()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setDebugMode(enableCheck)
      .setBufferSize(bufferSize)
      .setPSPartitionNum(psPartitionNum)
      .setInput(input)
      .setSrcNodeIndex(srcIndex)
      .setDstNodeIndex(dstIndex)
      .setDelimiter(sep)
      .setIsWeighted(isWeighted)
    motifCount.setHasEdgeType(hasEdgeType)
    motifCount.setOutputDir(output)

    if (userGroupInput != null) {
      val userGroup = GraphIO.load(userGroupInput, isWeighted=false, srcIndex, dstIndex, sep=sep)
        .select("src", "dst").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1)))
      motifCount.setUserGroup(userGroup)
    }
    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex = srcIndex,
      dstIndex = dstIndex, weightIndex = weightIndex, sep = sep, hasEdgeType = hasEdgeType, edgeTypeIndex = edgeTypeIndex)

    //collect app name and data size
    DataStatistics.setAppName(motifCount.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val mapping = motifCount.transform(df)
//    GraphIO.save(mapping, output)

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("WeightedMotifCountingDirectedV2")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
