package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.louvain.Louvain
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LouvainExample {

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("k-core")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    sc.setLogLevel("WARN")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/louvain"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY
    val numFold = 2
    val numOpt = 5
    val batchSize = 100
    val enableCheck = true
    val eps = 0.0
    val bufferSize = 100000
    val isWeighted = false
    val psPartitionNum = 2

    start(mode)
    val df = SparkSession.builder().getOrCreate()
      .read
      .option("sep", " ")
      .schema(StructType(Seq(StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false))))
      .csv(input)
    val louvain = new Louvain()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setNumFold(numFold)
      .setNumOpt(numOpt)
      .setBatchSize(batchSize)
      .setDebugMode(enableCheck)
      .setEps(eps)
      .setBufferSize(bufferSize)
      .setIsWeighted(isWeighted)
      .setPSPartitionNum(psPartitionNum)

    val mapping = louvain.transform(df)

    mapping.write.mode(SaveMode.Overwrite).parquet(output)
    stop()
  }
}
