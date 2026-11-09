package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.communitydetection.subgraph.SeedNeighbors
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SeedNeighborsLocalExample {

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")
    val input = params.getOrElse("input", "data/bc/edge")
    val output = params.getOrElse("output", "model/seedneighbors")

    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "DISK_ONLY"))
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "1").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val maxIteration = params.getOrElse("maxIteration", "1").toInt

    // params for node attrs files
    val seedsPath = params.getOrElse("seedsPath", "data/bc/seeds")
    val nodeIdInSeedsPath = params.getOrElse("nodeIdInSeedsPath", "0").toInt
    // discard nodes with degree greater than maxNodeDegree
    val maxNodeDegree = params.getOrElse("maxNodeDegree", "0").toInt
    // discard nodes by percentageThreshold
    val percentageThreshold = params.getOrElse("percentageThreshold", "0.75").toFloat

    assert(maxNodeDegree > 0 || percentageThreshold > 0,
      s"require maxNodeDegree or percentageThreshold should be greater than 0, " +
        s"get $maxNodeDegree and $percentageThreshold.")
    start(mode)
    val seedNbrs = new SeedNeighbors()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setNodeAttrPath(seedsPath)
      .setBatchSize(batchSize)
      .setUseBalancePartition(useBalancePartition)
      .setMaxIteration(maxIteration)

    seedNbrs.setOutput(output)

    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)

    //collect app name and data size

    // read and set nodeAttr RDD
    seedNbrs.setNodeAttr(readAttrs(seedsPath,nodeIdInSeedsPath,sep))
    seedNbrs.setMaxNodeDegree(maxNodeDegree)
    seedNbrs.setPercentageThreshold(percentageThreshold)

    seedNbrs.transform(df)
    stop()
  }

  def readAttrs(path: String, nodeId: Int, sep: String): RDD[Long] = {

    import com.tencent.angel.spark.ml.graph.utils.GraphIO
    val seeds = GraphIO.loadNodeSeed(path, nodeId, sep).rdd
      .map(x => (x.getLong(0)))
    seeds
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("seedNeighbors")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    //    sc.setCheckpointDir("cp")
    //    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
