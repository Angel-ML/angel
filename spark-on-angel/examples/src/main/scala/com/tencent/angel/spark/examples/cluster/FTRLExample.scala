package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.vector.LongFloatVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.core.metric.AUC
import com.tencent.angel.spark.ml.online_learning.FTRL
import com.tencent.angel.spark.ml.util.{DataLoader, LoadBalancePartitioner, SparkUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object FTRLExample {

  def start(): Unit = {
    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse("actionType", "train").toString
    if (actionType == "train" || actionType == "incTrain") {
      train(params)
    } else {
      predict(params)
    }
    stop()
  }

  def train(params: Map[String, String]): Unit = {

    val alpha = params.getOrElse("alpha", "2.0").toDouble
    val beta = params.getOrElse("beta", "1.0").toDouble
    val lambda1 = params.getOrElse("lambda1", "0.1").toDouble
    val lambda2 = params.getOrElse("lambda2", "5.0").toDouble
    val dim = params.getOrElse("dim", "-1").toLong
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val dataType = params.getOrElse("dataType", "libsvm")
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val numEpoch = params.getOrElse("numEpoch", "3").toInt
    val output = params.getOrElse("modelPath", "")
    val modelPath = params.getOrElse("model", "")
    val withBalancePartition = params.getOrElse("balance", "false").toBoolean
    val possionRate = params.getOrElse("possion", "1.0f").toFloat
    val bits = params.getOrElse("bits", "20").toInt
    val numPartitions = params.getOrElse("numPartitions", "100").toInt

    val opt = new FTRL(lambda1, lambda2, alpha, beta)
    val rowType = RowType.T_FLOAT_SPARSE_LONGKEY

    val conf = new SparkConf()
    if (modelPath.length > 0)
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, modelPath + "/back")
    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)

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


    data.persist(StorageLevel.DISK_ONLY)
    val size = data.count()

    val max = data.map(f => f.getX.asInstanceOf[LongFloatVector].getStorage().getIndices.max).max()
    val min = data.map(f => f.getX.asInstanceOf[LongFloatVector].getStorage().getIndices.min).min()

    println(s"num examples = ${size} min_index=$min max_index=$max")

    if (withBalancePartition)
      opt.init(min, max + 1, rowType, data.map(f => f.getX),
        new LoadBalancePartitioner(bits, numPartitions))
    else
      opt.init(min, max + 1, -1, rowType, new ColumnRangePartitioner())

    opt.setPossionRate(possionRate)

    if (modelPath.length > 0){
      opt.load(modelPath + "/back")
    }
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
      opt.saveWeight(output + "/weight")
      opt.save(output + "/back")
      println(s"saving z n and w finish")
    }
  }

  def predict(params: Map[String, String]): Unit = {

    val dim = params.getOrElse("dim", "149").toLong
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val dataType = params.getOrElse("dataType", "libsvm")
    val partNum = params.getOrElse("partNum", "10").toInt
    val isTraining = params.getOrElse("isTraining", "false").toBoolean
    val hasLabel = params.getOrElse("hasLabel", "true").toBoolean
    val modelPath = params.getOrElse("modelPath", "")
    val predictPath = params.getOrElse("predict", "")


    val opt = new FTRL()
    val conf = new SparkConf()
    conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, modelPath + "/weight")

    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)

    val inputData = sc.textFile(input)
    val data = dataType match {
      case "libsvm" =>
        inputData .map(s =>
          (DataLoader.parseLongFloat(s, dim, isTraining, hasLabel)))
      case "dummy" =>
        inputData .map(s =>
          (DataLoader.parseLongDummy(s, dim, isTraining, hasLabel)))
    }

    val max = data.map(f => f.getX.asInstanceOf[LongFloatVector].getStorage().getIndices.max).max()
    val min = data.map(f => f.getX.asInstanceOf[LongFloatVector].getStorage().getIndices.min).min()
    opt.init(min, max + 1, -1, RowType.T_FLOAT_SPARSE_LONGKEY, new ColumnRangePartitioner())

    if (modelPath.size > 0) {
      opt.load(modelPath + "/weight")
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
