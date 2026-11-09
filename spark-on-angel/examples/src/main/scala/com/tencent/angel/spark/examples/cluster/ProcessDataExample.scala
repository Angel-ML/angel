package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.tree.kdtree.predictor.KDTPredictor
import com.tencent.angel.spark.ml.tree.kdtree.trainer.KDTTrainer
import com.tencent.angel.spark.ml.tree.param.KDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import org.apache.spark.{SparkConf, SparkContext}

object ProcessDataExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse("actionType", "train")
    actionType match {
      case "train" => train(params)
      case _ => throw new AngelException("Unsupported action type: " + actionType)
    }
  }

  def train(params: Map[String, String]): Unit = {
    @transient val conf = new SparkConf()

    val trainPath = params("input")
    val modelPath = params("out")
    val batchSize = params.getOrElse("batchSize", "1000").toInt
    val numPartitions = params.getOrElse("numPartitions", "100").toInt

    @transient implicit val sc = new SparkContext(conf)

    val data = sc.textFile(trainPath).repartition(numPartitions)
    data.saveAsTextFile(modelPath)


  }

}