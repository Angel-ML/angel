package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.node2vec.Node2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object Node2VecExample {
  val customSchema = StructType(Array(
    StructField("src", LongType, nullable = false),
    StructField("dst", LongType, nullable = false))
  )

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "E:\\code\\angel\\data\\bc\\edge").toString
    val output = params.getOrElse("output", "E:\\code\\angel\\n2v").toString
    val initBatchSize = params.getOrElse("initBatchSize", "128").toInt
    val psPartNum = params.getOrElse("psPartNum", "2").toInt
    val dataPartNum = params.getOrElse("dataPartNum", "4").toInt
    val maxNodeId = params.getOrElse("maxNodeId", "10312").toInt
    //val mode = params.getOrElse("mode", "local[4]")
    val walkLength = params.getOrElse("walkLength", "30").toInt
    val pValue = params.getOrElse("pValue", "0.8").toDouble
    val qValue = params.getOrElse("qValue", "1.2").toDouble
    val needReplicaEdge = params.getOrElse("needReplicaEdge", "true").toBoolean
    val degreeBinSize = params.getOrElse("degreeBinSize", "100").toInt
    val hitRatio = params.getOrElse("hitRatio", "0.5").toDouble
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt

    // Spark setup
    val spark = SparkSession.builder()
      .appName("node2vec")
      .getOrCreate()

    // spark.sparkContext.setLogLevel("ERROR")
    // val spConf = spark.sparkContext.getConf

    // PS setup
    println("Start PS ...")
    Node2Vec.startPS(spark.sparkContext)
    println("PS Started!")

    val start = System.currentTimeMillis()
    val data = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", " ")
      .schema(customSchema)
      .load(input)

    val n2v = new Node2Vec()
      .setPSPartitionNum(psPartNum)
      .setPartitionNum(dataPartNum)
      .setBatchSize(initBatchSize)
      .setWalkLength(walkLength)
      .setPValue(pValue)
      .setQValue(qValue)
      .setNeedReplicaEdge(needReplicaEdge)
      .setDegreeBinSize(degreeBinSize)
      .setHitRatio(hitRatio)
      .setPullBatchSize(pullBatchSize)

    println("begin to fit|train ...")
    val model = n2v.fit(data)
    println("fit|train finished!")

    println("start to save model")
    model.write.overwrite().save(output)
    println(s"finish to save model")

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
