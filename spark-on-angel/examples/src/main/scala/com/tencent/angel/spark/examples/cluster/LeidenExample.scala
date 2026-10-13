package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.graph.community.leiden.Leiden
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object LeidenExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    val spark = start()

    val leiden = new Leiden(params)

    val sc = spark.sparkContext
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val df = GraphIO.load(leiden.getInput, isWeighted = leiden.getIsWeighted,
      srcIndex = leiden.getSrcNodeIndex(), dstIndex = leiden.getDstNodeIndex(),
      weightIndex = leiden.getWeightIndex(), sep = leiden.getItemSep)


    PSContext.getOrCreate(df.sparkSession.sparkContext)

    val mapping = leiden.transform(df)

    mapping.show(truncate = false)

    GraphIO.save(mapping, leiden.getOutput)

    stop()
  }


  def start(): SparkSession = {
    val conf = new SparkConf()
    conf.setAppName("Leiden")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
