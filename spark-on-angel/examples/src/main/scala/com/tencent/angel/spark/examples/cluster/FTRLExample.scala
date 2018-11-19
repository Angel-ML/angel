package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.ml.math2.vector.{LongDoubleVector, Vector}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.online_learning.{FTRL, SparseLRModel}
import com.tencent.angel.spark.ml.util.DataLoader
import org.apache.spark.{SparkConf, SparkContext}

object FTRLExample {
  //init param parameters for alpha, beta, lambda1, lambda2
  val ALPHA = "alpha"
  val BETA = "beta"
  val LAMBDA1 = "lambda1"
  val lAMBDA2 = "lambda2"
  val RHO = "rho"
  val RHO1 = "rho1"
  val RHO2 = "rho2"

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val alpha = params.getOrElse(ALPHA, "1.0").toDouble
    val beta = params.getOrElse(BETA, "1.0").toDouble
    val lambda1 = params.getOrElse(LAMBDA1, "1.0").toDouble
    val lambda2 = params.getOrElse(lAMBDA2, "1.0").toDouble
    val rho1 = params.getOrElse(RHO1, "1.0").toDouble
    val rho2 = params.getOrElse(RHO2, "1.0").toDouble
    val dim = params.getOrElse("dim", "11").toLong
    val input = params.getOrElse("input", "")
    val modelPath = params.getOrElse("output", "")

    val conf = new SparkConf()
    val sc   = new SparkContext(conf)

    PSContext.getOrCreate(sc)

    val opt = new FTRL(lambda1, lambda2, alpha, beta)
    opt.initPSModel(dim)

    val data = sc.textFile(input).map(s => DataLoader.parseLongDouble(s, dim))
      .map(f => (f.getX, f.getY))

    val totalLoss = data.mapPartitions { case iterator =>
      val batch = iterator.toArray
      val loss = opt.optimize(batch, calcuGradientLoss)
      Iterator.single(loss)
    }.collect()

    println(s"average loss ${totalLoss.sum / totalLoss.length}")

    if (modelPath.length > 0) {
      val model = SparseLRModel(opt.weight)
      model.save(modelPath)
    }
  }

  private def calcuGradientLoss(w: LongDoubleVector, label: Double, feature: Vector): (LongDoubleVector, Double) = {
    val margin = -w.dot(feature)
    val gradientMultiplier = 1.0 / (1.0 + math.exp(margin)) - label
    val grad = feature.mul(gradientMultiplier)

    val loss = if (label > 0) {
      math.log1p(math.exp(margin))
    } else {
      math.log1p(math.exp(margin)) - margin
    }

    (grad.asInstanceOf[LongDoubleVector], loss)
  }
}
