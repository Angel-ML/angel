package com.tencent.angel.spark.examples.cluster

import java.util
import java.util.List

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.vector.{LongDummyVector, LongFloatVector}
import com.tencent.angel.ml.matrix.{MatrixContext, PartitionMeta, RowType}
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.core.metric.AUC
import com.tencent.angel.spark.ml.online_learning.FTRL
import com.tencent.angel.spark.ml.util.{DataLoader, SparkUtils}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object FTRLExample {

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

    val conf = new SparkConf()

    if (modelPath.length > 0)
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, modelPath + "/back")

    val sc = new SparkContext(conf)

    PSContext.getOrCreate(sc)

    // We use more partitions to achieve dynamic load balance
    val partNum = (SparkUtils.getNumCores(SparkContext.getOrCreate().getConf) * 6.15).toInt

    val data = sc.textFile(input).repartition(partNum)
      .map(s => (DataLoader.parseLongDummy(s, dim), DataLoader.parseLabel(s, false)))
      .map {
        f =>
          f._1.setY(f._2)
          f._1
      }

    data.persist(StorageLevel.DISK_ONLY)
    val size = data.count()

    val max = data.map(f => f.getX.asInstanceOf[LongDummyVector].getIndices.max).max()
    val min = data.map(f => f.getX.asInstanceOf[LongDummyVector].getIndices.min).min()
    val nnz = data.flatMap(f => f.getX.asInstanceOf[LongDummyVector].getIndices.distinct).map(f => (f, 1))
        .reduceByKey(_ + _).count()

    println(s"num examples = ${size} min_index=$min max_index=$max dim=$nnz")

    val opt = new FTRL(lambda1, lambda2, alpha, beta)
    opt.init(min, max + 1, nnz, RowType.T_FLOAT_SPARSE_LONGKEY)

    if (modelPath.length > 0)
      opt.load(modelPath + "/back")

    for (epoch <- 1 to numEpoch) {
      val totalLoss = data.mapPartitions {
        case iterator =>
          val loss = iterator
            .sliding(batchSize, batchSize)
            .map(f => opt.optimize(f.toArray)).sum
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
      opt.weight
      opt.saveWeight(output)
      opt.save(output + "/back")
      println(s"saving z n and w finish")
    }

    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
