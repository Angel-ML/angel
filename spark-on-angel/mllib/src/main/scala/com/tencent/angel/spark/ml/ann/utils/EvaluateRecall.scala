package com.tencent.angel.spark.ml.ann.utils

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object EvaluateRecall {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)
    
    val input = params.getOrElse("input", null)
    val groundtruth = params.getOrElse("groundtruth", null)
    val output = params.getOrElse("output", null)
    
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)
    
    val outs = DataUtils.loadKNNResults(SparkSession.builder().getOrCreate(), input)
    val truths = DataUtils.loadKNNResults(SparkSession.builder().getOrCreate(), groundtruth)
    
    val recalls = recall(outs, truths).map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
    
    val schema = StructType(Seq(StructField("query", StringType, nullable = false),
      StructField("recall", FloatType, nullable = false)))
    
    val df = SparkSession.builder().getOrCreate().createDataFrame(recalls, schema)
    
    DataUtils.saveResult(df, output)
    
    stop()
  }
  
  def recall(output: Dataset[_], groundTruth: Dataset[_]): RDD[(String, Float)] = {
    val outs = output.select("query", "nbr:dis").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getString(0), row.getString(1)))
    
    println(s"outs num: ${outs.count()}")
    
    val truths = groundTruth.select("query", "nbr:dis").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getString(0), row.getString(1)))
    
    println(s"truths num: ${truths.count()}")
    
    val rec =
      outs.leftOuterJoin(truths).map{ it =>
        assert(it._2._2.isDefined, s"output should be subset of ground truth..")
        val out = it._2._1.split(" ").map(_.split(":")(0))
        val truth = it._2._2.get.split(" ").map(_.split(":")(0))
        
        (it._1, out.count(t => truth.contains(t)).toFloat / out.length.toFloat)
      }
    
    println(s"average recall: ${rec.map(_._2).mean()}")
    
    rec
  }
  
  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("eval-recall")
    val sc = new SparkContext(conf)
    sc
  }
  
  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}
