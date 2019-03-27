package com.tencent.angel.spark.examples.local

import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.core.metric.AUC
import com.tencent.angel.spark.ml.online_learning.FtrlFM
import com.tencent.angel.spark.ml.util.DataLoader
import org.apache.spark.{SparkConf, SparkContext}

object FtrlFMExample {

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


  def main(args: Array[String]): Unit = {
    start()

    val params = ArgsUtil.parse(args)
    val alpha = params.getOrElse("alpha", "2.0").toDouble
    val beta = params.getOrElse("beta", "1.0").toDouble
    val lambda1 = params.getOrElse("lambda1", "0.1").toDouble
    val lambda2 = params.getOrElse("lambda2", "100.0").toDouble
    val dim = params.getOrElse("dim", "10000").toInt
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val partNum = params.getOrElse("partNum", "10").toInt
    val numEpoch = params.getOrElse("numEpoch", "3").toInt
    val modelPath = params.getOrElse("output", "model")
    val factor: Int = 10

    val opt = new FtrlFM(lambda1, lambda2, alpha, beta)
    opt.init(0, dim, -1, RowType.T_FLOAT_DENSE, factor, new ColumnRangePartitioner())

    val sc = SparkContext.getOrCreate()
    val data = sc.textFile(input).repartition(partNum)
      .map(s => (DataLoader.parseIntFloat(s, dim), DataLoader.parseLabel(s, false)))
      .map {
        f =>
          f._1.setY(f._2)
          f._1
      }
    val size = data.count()

    for (epoch <- 1 until numEpoch) {
      val totalLoss = data.mapPartitions {
        case iterator =>
          val loss = iterator
            .sliding(batchSize, batchSize)
            .zipWithIndex
            .map(f => opt.optimize(f._2, f._1.toArray)).sum
          Iterator.single(loss)
      }.sum()

      val scores = data.mapPartitions {
        case iterator =>
          iterator.sliding(batchSize, batchSize)
            .flatMap(batch => opt.predict(batch.toArray))}
      val auc = new AUC().calculate(scores)


      println(s"epoch=$epoch loss=${totalLoss / size} auc=$auc")
    }

    opt.showSecond()

    if (modelPath.length > 0) {
//      opt.weight
//      opt.save(modelPath)
//      opt.saveWeight(modelPath)
    }
    stop()
  }
}
