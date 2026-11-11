package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.util.{LogUtils, ParamKeys}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.recommendation.angel.ALS
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
/**
 * Alternating Least Squares (ALS) matrix factorization
 *
 * 参数说明:
 *
 * -----input parameter-----
 * input          input path
 * userCol        user列(从0开始计数), default: 0
 * itemCol        item列(从0开始计数), default: 1
 * ratingCol      rating列(从0开始计数), default: 2
 * partitionNUm     partition数目, 并行数
 * sampleRate     采样率. 默认:1.0 [optional]
 *
 * -----algorithm parameter----
 * rank
 * regParam       正则项系数
 * maxIter        最大迭代次数
 *
 * -----system parameter-----
 * mode          local or yarn
 * actionType    train/predict
 * modelPath     model HDFS path. train阶段, 模型保存到该路径, predict阶段, 从该路径load模型
 * testSet       testSet path for evaluate trained model. if null, then skip test step
 * output        预测的输出路径, 在train阶段, output == null
 */

object ALSCluster {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime(s"Start Angel ALS Processing...")
    val params = coreArgsUtil.parse(args)

    val mode = params.getOrElse(ParamKeys.MODE, "yarn")
    val input = params.getOrElse(ParamKeys.INPUT, null)
    val userCol = params.getOrElse("userCol", "0").toInt
    val itemCol = params.getOrElse("itemCol", "1").toInt
    val ratingCol = params.getOrElse("ratingCol", "2").toInt
    val sampleRate = params.getOrElse(ParamKeys.SAMPLE_RATE, "1.0").toDouble
    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "10").toInt

    val rank = params.getOrElse("rank", "5").toInt
    val regParam = params.getOrElse(ParamKeys.REG_PARAM, "0.1").toDouble
    val maxIter = params.getOrElse(ParamKeys.MAX_ITER, "20").toInt
    val implicitPrefs = params.getOrElse("implicitPrefs", "false").toBoolean
    val nonNegative = params.getOrElse("nonNegative", "false").toBoolean
    val alpha = params.getOrElse("alpha", "1.0").toDouble

    val recommendPath = params.getOrElse("recommendPath", null)
    val userOrItem = params.getOrElse("userOrItem", "user") // user or item
    val recommendNum = params.getOrElse("recommendNum", "100").toInt
    val blockSize = params.getOrElse("blockSize", "4096").toInt

    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, null)
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)
    val testSetPath = params.getOrElse(ParamKeys.TEST_SET, null)

    val output = params.getOrElse(ParamKeys.OUTPUT, null)
    val outputUserDF = params.getOrElse("outputUserDF", null)
    val outputItemDF = params.getOrElse("outputItemDF", null)

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }



    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    var executorJvmOptions = conf.get("spark.executor.extraJavaOptions", "")
    executorJvmOptions += " -Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.F2jBLAS"
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val als = new ALS()
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setRatingCol(ratingCol)
      .setPartitionNum(partitionNum)
      .setSampleRate(sampleRate)
      .setRank(rank)
      .setRegParam(regParam)
      .setMaxIter(maxIter)
      .setImplicitPrefs(implicitPrefs)
      .setNonNegative(nonNegative)
      .setAlpha(alpha)
      .setRecommendPath(recommendPath)
      .setRecommendNum(recommendNum)
      .setRecommendForUser(if (userOrItem == "user") true else false)
      .setBlockSize(blockSize)
      .setSep(sep)
      .setOutputItemDF(outputItemDF)
      .setOutputUserDF(outputUserDF)


    als.process(actionType, input, modelPath, testSetPath, output)
    LogUtils.logTime(s"${this.getClass.getSimpleName} finished.")
  }
}
