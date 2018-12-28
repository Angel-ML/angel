package com.tencent.angel.spark.ml.tree.gbdt

import com.tencent.angel.spark.ml.tree.common.Global.Conf._
import com.tencent.angel.spark.ml.tree.gbdt.learner.SparkFPGBDTTrainer
import com.tencent.angel.spark.ml.tree.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import org.apache.spark.{SparkConf, SparkContext}

object GBDT extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    implicit val sc = SparkContext.getOrCreate(conf)

    val param = new GBDTParam
    param.numClass = conf.getInt(ML_NUM_CLASS, DEFAULT_ML_NUM_CLASS)
    param.numFeature = conf.get(ML_NUM_FEATURE).toInt
    param.featSampleRatio = conf.getDouble(ML_FEATURE_SAMPLE_RATIO, DEFAULT_ML_FEATURE_SAMPLE_RATIO).toFloat
    param.numWorker = conf.get(ML_NUM_WORKER).toInt
    param.numThread = conf.getInt(ML_NUM_THREAD, DEFAULT_ML_NUM_THREAD)
    param.lossFunc = conf.get(ML_LOSS_FUNCTION)
    param.evalMetrics = conf.get(ML_EVAL_METRIC, DEFAULT_ML_EVAL_METRIC).split(",").map(_.trim).filter(_.nonEmpty)
    param.learningRate = conf.getDouble(ML_LEARN_RATE, DEFAULT_ML_LEARN_RATE).toFloat
    param.histSubtraction = conf.getBoolean(ML_GBDT_HIST_SUBTRACTION, DEFAULT_ML_GBDT_HIST_SUBTRACTION)
    param.lighterChildFirst = conf.getBoolean(ML_GBDT_LIGHTER_CHILD_FIRST, DEFAULT_ML_GBDT_LIGHTER_CHILD_FIRST)
    param.fullHessian = conf.getBoolean(ML_GBDT_FULL_HESSIAN, DEFAULT_ML_GBDT_FULL_HESSIAN)
    param.numSplit = conf.getInt(ML_GBDT_SPLIT_NUM, DEFAULT_ML_GBDT_SPLIT_NUM)
    param.numTree = conf.getInt(ML_GBDT_TREE_NUM, DEFAULT_ML_GBDT_TREE_NUM)
    param.maxDepth = conf.getInt(ML_GBDT_MAX_DEPTH, DEFAULT_ML_GBDT_MAX_DEPTH)
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = conf.getInt(ML_GBDT_MAX_NODE_NUM, maxNodeNum) min maxNodeNum
    param.minChildWeight = conf.getDouble(ML_GBDT_MIN_CHILD_WEIGHT, DEFAULT_ML_GBDT_MIN_CHILD_WEIGHT).toFloat
    param.minNodeInstance = conf.getInt(ML_GBDT_MIN_NODE_INSTANCE, DEFAULT_ML_GBDT_MIN_NODE_INSTANCE)
    param.minSplitGain = conf.getDouble(ML_GBDT_MIN_SPLIT_GAIN, DEFAULT_ML_GBDT_MIN_SPLIT_GAIN).toFloat
    param.regAlpha = conf.getDouble(ML_GBDT_REG_ALPHA, DEFAULT_ML_GBDT_REG_ALPHA).toFloat
    param.regLambda = conf.getDouble(ML_GBDT_REG_LAMBDA, DEFAULT_ML_GBDT_REG_LAMBDA).toFloat max 1.0f
    param.maxLeafWeight = conf.getDouble(ML_GBDT_MAX_LEAF_WEIGHT, DEFAULT_ML_GBDT_MAX_LEAF_WEIGHT).toFloat
    println(s"Hyper-parameters:\n$param")

    try {
      val trainer = new SparkFPGBDTTrainer(param)
      //val input = conf.get(ML_TRAIN_DATA_PATH)
      //val validRatio = conf.getDouble(ML_VALID_DATA_RATIO, DEFAULT_ML_VALID_DATA_RATIO)
      //trainer.loadData(input, validRatio)
      val trainInput = conf.get(ML_TRAIN_DATA_PATH)
      val validInput = conf.get(ML_VALID_DATA_PATH)
      trainer.initialize(trainInput, validInput)
      trainer.train()
    } catch {
      case e: Exception =>
        println(e.toString)
        e.printStackTrace()
    } finally {
      //while (1 + 1 == 2) {}
    }

  }

}
