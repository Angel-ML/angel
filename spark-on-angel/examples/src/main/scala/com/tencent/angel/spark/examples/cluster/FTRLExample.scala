package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.vector.{LongDoubleVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.core.metric.AUC
import com.tencent.angel.spark.ml.online_learning.{FTRL, SparseLRModel}
import com.tencent.angel.spark.ml.util.{DataLoader, SparkUtils}
import org.apache.spark.{SparkConf, SparkContext}

object FTRLExample {

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val alpha = params.getOrElse("alpha", "2.0").toDouble
    val beta = params.getOrElse("beta", "1.0").toDouble
    val lambda1 = params.getOrElse("lambda1", "0.1").toDouble
    val lambda2 = params.getOrElse("lambda2", "5.0").toDouble
    val dim = params.getOrElse("dim", "149").toLong
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val numEpoch = params.getOrElse("numEpoch", "3").toInt
    val output = params.getOrElse("output", "")
    val modelPath = params.getOrElse("model", "")

    val conf = new SparkConf()

    if (modelPath.length > 0)
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, modelPath + "/back")

    val sc = new SparkContext(conf)

    PSContext.getOrCreate(sc)

    // We use more partitions to achieve dynamic load balance
    val partNum = (SparkUtils.getNumExecutors(SparkContext.getOrCreate().getConf) * 6.15).toInt

    val opt = new FTRL(lambda1, lambda2, alpha, beta)
    opt.init(dim, RowType.T_DOUBLE_SPARSE_LONGKEY)

    if (modelPath.length > 0)
      opt.load(modelPath + "/back")

    val data = sc.textFile(input).repartition(partNum)
      .map(s => (DataLoader.parseLongDouble(s, dim), DataLoader.parseLabel(s, false)))
      .map {
        f =>
          f._1.setY(f._2)
          f._1
      }

    val size = data.count()

    for (epoch <- 1 to numEpoch) {
      val totalLoss = data.mapPartitions {
        case iterator =>
          val loss = iterator.map(f => (f.getX, f.getY))
            .sliding(batchSize, batchSize)
            .map(f => opt.optimize(f.toArray, calcGradientLoss)).sum
          Iterator.single(loss)
      }.sum()

      val scores = data.mapPartitions {
        case iterator =>
          iterator.sliding(batchSize, batchSize)
              .map(f => opt.predict(f.toArray)).flatMap(f => f)
      }
      val auc = new AUC().calculate(scores)

      println(s"epoch=$epoch loss=${totalLoss / size} auc=$auc")
    }

    if (output.length > 0) {
      println(s"saving model to path $output")
      opt.weight
      opt.saveWeight(output)
      opt.save(output + "/back")
      println(s"saving z n and w finish")
    }

    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  private def calcGradientLoss(w: LongDoubleVector, label: Double, feature: Vector): (LongDoubleVector, Double) = {
    val margin = -w.dot(feature)
    val gradientMultiplier = 1.0 / (1.0 + math.exp(margin)) - label
    val grad = feature.mul(gradientMultiplier).asInstanceOf[LongDoubleVector]
    val loss = if (label > 0) log1pExp(margin) else log1pExp(margin) - margin
    (grad, loss)
  }

  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}
