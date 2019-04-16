package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.kcore.KCore
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KCoreExample {

  def main(args: Array[String]): Unit = {
    val switchRate = 0.1
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/kcore"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 100
    val psPartitionNum = 2

    start(mode)
    val df = SparkSession.builder().getOrCreate()
      .read
      .option("sep", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false))))
      .csv(input)
    val kcore = new KCore()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPSPartitionNum(psPartitionNum)

    val mapping = kcore.transform(df)

    mapping.write.mode(SaveMode.Overwrite).parquet(output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("k-core")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("cp")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
