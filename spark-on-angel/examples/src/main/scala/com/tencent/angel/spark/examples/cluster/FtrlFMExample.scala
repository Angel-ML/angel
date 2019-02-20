package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.core.metric.AUC
import com.tencent.angel.spark.ml.online_learning.FtrlFM
import com.tencent.angel.spark.ml.util.{DataLoader, SparkUtils}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object FtrlFMExample {

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val alpha = params.getOrElse("alpha", "2.0").toDouble
    val beta = params.getOrElse("beta", "1.0").toDouble
    val lambda1 = params.getOrElse("lambda1", "0.1").toDouble
    val lambda2 = params.getOrElse("lambda2", "5.0").toDouble
    val dim = params.getOrElse("dim", "-1").toInt
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val numEpoch = params.getOrElse("numEpoch", "3").toInt
    val output = params.getOrElse("output", "")
    val modelPath = params.getOrElse("model", "")
    val factor = params.getOrElse("factor", "10").toInt

    val conf = new SparkConf()

    if (modelPath.length > 0)
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, modelPath + "/back")

    val sc = new SparkContext(conf)

    PSContext.getOrCreate(sc)

    val data = sc.textFile(input).repartition((SparkUtils.getNumCores(conf) * 6.15).toInt)
      .map(s => (DataLoader.parseIntFloat(s, dim), DataLoader.parseLabel(s, false)))
      .map {
        f =>
          f._1.setY(f._2)
          f._1
      }

    data.persist(StorageLevel.DISK_ONLY)
    val size = data.count()

    val max = data.map(f => f.getX.asInstanceOf[IntFloatVector].getStorage().getIndices.max.toLong).max()
    val min = data.map(f => f.getX.asInstanceOf[IntFloatVector].getStorage().getIndices.min.toLong).min()

    println(s"num examples = ${size} min_index=$min max_index=$max")

    val opt = new FtrlFM(lambda1, lambda2, alpha, beta)

    val rowType = RowType.T_FLOAT_DENSE

    opt.init(0, max+1, -1, rowType, factor, new ColumnRangePartitioner())

    for (epoch <- 1 to numEpoch) {
      val totalLoss = data.mapPartitions {
        case iterator =>
          val loss = iterator
            .sliding(batchSize, batchSize)
            .zipWithIndex
            .map(f => opt.optimize(f._2, f._1.toArray)).sum
          Iterator.single(loss)
      }.sum()

      val scores = data.sample(false, 0.01, 42).mapPartitions {
        case iterator =>
          iterator.sliding(batchSize, batchSize)
            .flatMap(f => opt.predict(f.toArray))
      }
      val auc = new AUC().calculate(scores)

      println(s"epoch=$epoch loss=${totalLoss / size} auc=$auc")
    }

    if (output.length > 0) {
      println(s"saving model to path $output")
      opt.saveWeight(output)
    }

    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
