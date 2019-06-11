package com.tencent.angel.spark.examples.local

import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.core.metric.AUC
import com.tencent.angel.spark.ml.online_learning.FTRL
import com.tencent.angel.spark.ml.util.DataLoader
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

object FTRLExample {

  def main(args: Array[String]): Unit = {
    start()
    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse("actionType", "train").toString
    if (actionType == "train" || actionType == "incTrain") {
      train(params)
    } else {
      predict(params)
    }
    stop()
  }

  def start(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("PSVector Examples")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def train(params: Map[String, String]): Unit = {

    val alpha = params.getOrElse("alpha", "2.0").toDouble
    val beta = params.getOrElse("beta", "1.0").toDouble
    val lambda1 = params.getOrElse("lambda1", "0.1").toDouble
    val lambda2 = params.getOrElse("lambda2", "100.0").toDouble
    val dim = params.getOrElse("dim", "149").toLong
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val dataType = params.getOrElse("dataType", "libsvm")
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val partNum = params.getOrElse("partNum", "10").toInt
    val numEpoch = params.getOrElse("numEpoch", "3").toInt
    val output = params.getOrElse("output", "file:///model")
    val loadPath = params.getOrElse("load", "")

    val opt = new FTRL(lambda1, lambda2, alpha, beta)
    opt.init(dim, RowType.T_FLOAT_SPARSE_LONGKEY)

    val sc = SparkContext.getOrCreate()
    val inputData = sc.textFile(input)
    val data = dataType match {
      case "libsvm" =>
        inputData .map(s => (DataLoader.parseLongFloat(s, dim), DataLoader.parseLabel(s, false)))
          .map {
            f =>
              f._1.setY(f._2)
              f._1
          }
      case "dummy" =>
        inputData .map(s => (DataLoader.parseLongDummy(s, dim), DataLoader.parseLabel(s, false)))
          .map {
            f =>
              f._1.setY(f._2)
              f._1
          }
    }
    val size = data.count()

    if (loadPath.size > 0)
      opt.load(loadPath + "/back")

    for (epoch <- 1 until numEpoch) {
      val totalLoss = data.mapPartitions {
        case iterator =>
          val loss = iterator
            .sliding(batchSize, batchSize)
            .map(f => opt.optimize(f.toArray)).sum
          Iterator.single(loss)
      }.sum()

      val scores = data.mapPartitions {
        case iterator =>
          opt.predict(iterator.toArray).iterator}
      val auc = new AUC().calculate(scores)

      println(s"epoch=$epoch loss=${totalLoss / size} auc=$auc")
    }

    if (output.length > 0) {
      val weight = opt.weight
      opt.save(output + "/back")
      opt.saveWeight(output + "/weight")
    }
  }

  def predict(params: Map[String, String]): Unit = {

    val dim = params.getOrElse("dim", "149").toLong
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val dataType = params.getOrElse("dataType", "libsvm")
    val partNum = params.getOrElse("partNum", "10").toInt
    val isTraining = params.getOrElse("isTraining", "false").toBoolean
    val hasLabel = params.getOrElse("hasLabel", "true").toBoolean
    val loadPath = params.getOrElse("load", "file:///model")
    val predictPath = params.getOrElse("predict", "file:///model/predict")


    val opt = new FTRL()
    opt.init(dim, RowType.T_FLOAT_SPARSE_LONGKEY)

    val sc = SparkContext.getOrCreate()

    val inputData = sc.textFile(input)
    val data = dataType match {
      case "libsvm" =>
        inputData .map(s =>
          (DataLoader.parseLongFloat(s, dim, isTraining, hasLabel)))
      case "dummy" =>
        inputData .map(s =>
          (DataLoader.parseLongDummy(s, dim, isTraining, hasLabel)))
    }

    if (loadPath.size > 0) {
      opt.load(loadPath + "/weight")
    }

    val scores = data.mapPartitions {
      case iterator =>
        opt.predict(iterator.toArray, false).iterator
    }

    val path = new Path(predictPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    scores.saveAsTextFile(predictPath)
  }

}
