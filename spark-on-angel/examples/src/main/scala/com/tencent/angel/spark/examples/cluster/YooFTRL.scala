package com.tencent.angel.spark.examples.cluster

import com.google.common.primitives.UnsignedLong
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.core.metric.AUC
import com.tencent.angel.spark.ml.online_learning.FTRL
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import org.apache.spark.{SparkConf, SparkContext}

object YooFTRL {

  def parseSample(text: String, dim: Long, blackListFeature: Set[String], enableCrossScore: Boolean, minFeatureLen: Int): LabeledData = {
    if (null == text) return null
    val sampleSplit = text.split("\t", -1)
    val y = sampleSplit(0).toDouble

    //    if(sampleSplit.length != 3) throw new UnexpectedException("error parsing: " + text)
    val splitsOri = sampleSplit(2).split(" ")

    if (splitsOri.length < minFeatureLen) return null

    val splits = splitsOri.map(x => x.trim().split(":")).filter(x => !blackListFeature.contains(x(0)))

    if (splits.length < 1) return null
    val len = splits.length

    val keys = new Array[Long](len + 1)
    val vals = new Array[Float](len + 1)

    splits.zipWithIndex.foreach { case (kv: Array[String], indx2: Int) =>
      val featureHashId = if (kv(0).contains('_')) kv(0).substring(2) else kv(0)
      keys(indx2) = UnsignedLong.valueOf(featureHashId).longValue()
      vals(indx2) = if (enableCrossScore) kv(1).toFloat else 1.0F
    }
    keys(len) = 0L
    vals(len) = 1.0F
    val x = VFactory.sparseLongKeyFloatVector(dim, keys, vals)
    new LabeledData(x, y)
  }

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val alpha = params.getOrElse("alpha", "2.0").toDouble
    val beta = params.getOrElse("beta", "1.0").toDouble
    val lambda1 = params.getOrElse("lambda1", "0.1").toDouble
    val lambda2 = params.getOrElse("lambda2", "5.0").toDouble
    val dim = params.getOrElse("dim", "-1").toLong
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val numEpoch = params.getOrElse("numEpoch", "3").toInt
    val output = params.getOrElse("output", "")
    val modelPath = params.getOrElse("model", "")
    val black = params.getOrElse("blackListFeature", "").split(",").toSet
    val cross = params.getOrElse("enableCrossScore", "true").toBoolean
    val minFeatureLen = params.getOrElse("minFeatureLen", "10").toInt

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    PSContext.getOrCreate(sc)

    val data = sc.textFile(input).map(f => parseSample(f, dim, black, cross, minFeatureLen)).filter(f => f != null)

//    val size = data.count()

//    val max = data.map(f => f.getX.asInstanceOf[LongFloatVector].getStorage().getIndices.max).max()
//    val min = data.map(f => f.getX.asInstanceOf[LongFloatVector].getStorage().getIndices.min).min()
    //    val nnz = data.flatMap(f => f.getX.asInstanceOf[LongFloatVector].getStorage().getIndices.distinct).map(f => (f, 1))
    //        .reduceByKey(_ + _).count()

//    println(s"num examples = ${size}")

    val opt = new FTRL(lambda1, lambda2, alpha, beta)
    opt.init(Long.MinValue, Long.MaxValue,
      RowType.T_FLOAT_SPARSE_LONGKEY, data.map(f => f.getX),
      new LoadBalancePartitioner(48, 500))

    for (epoch <- 1 to numEpoch) {
      val (totalLoss, size) = data.mapPartitions {
        case iterator =>
          val loss = iterator
            .sliding(batchSize, batchSize)
            .map(f => opt.optimize(f.toArray)).sum
          Iterator.single(loss, iterator.size.toLong)
      }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))

      val scores = data.sample(false, 0.01, 42).mapPartitions {
        case iterator =>
          iterator.sliding(batchSize, batchSize)
            .flatMap(f => opt.predict(f.toArray))
      }
      val auc = new AUC().calculate(scores)

      println(s"epoch=$epoch loss=${totalLoss / size} auc=$auc")
    }

    opt.weight

    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
