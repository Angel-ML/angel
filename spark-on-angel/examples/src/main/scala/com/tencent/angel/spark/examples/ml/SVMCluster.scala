package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.classification.svm.SVM
import com.tencent.angel.spark.ml.util._
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
/**
 * Created by isakjiang on 2022-04-25.
 */

/**
 *
 * SVM 支持向量机
 *
 * 参数说明:
 *
 * -----input parameter-----
 * input         input path
 * labelCol      标签列(从0开始计数), default: 0
 * beginCol      特征的起始列(从0开始计数, 包含当前列), default: 1
 * endCol        特征的终止列(从0开始计数, 包含当前列), default: length - 1
 * partitionNUm   partition数目, 并行数
 * sampleRate    采样率. 默认:1.0 [optional]
 *
 * ----algorithm parameter----
 * regParam      正则项系数
 * stepSize      SGD迭代步长
 * miniBatch     范围:0~1.0, 用于miniBatch的数据比例
 * maxIter       最大迭代次数
 *
 * ----system parameter-----
 * mode          local or yarn-cluster
 * actionType    train/predict
 * modelPath     model HDFS path. train阶段, 模型保存到该路径, predict阶段, 从该路径load模型
 * testSet       testSet path for evaluate trained model. if null, then skip test step
 * output        预测的输出路径, 在train阶段, output == null
 */

object SVMCluster {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime("Start SVM Processing...")

    val params = coreArgsUtil.parse(args)

    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val partitionNum = params.getOrElse("partitionNum", "4").toInt
    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, MAX_INT_STR).toInt

    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, null)
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)
    val PMMLmodelPath = params.getOrElse("PMMLmodelPath", null)
    val testSetPath = params.getOrElse(ParamKeys.TEST_SET, null)
    val output = params.getOrElse(ParamKeys.OUTPUT, null)

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }



    val regParam = params.getOrElse(ParamKeys.REG_PARAM, "0.01").toDouble
    val stepSize = params.getOrElse(ParamKeys.STEP_SIZE, "1.0").toDouble
    val miniBatch = params.getOrElse(ParamKeys.MINI_BATCH_FRACTION, "1.0").toDouble
    val maxIter = params.getOrElse(ParamKeys.MAX_ITER, "100").toInt

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val svm = new SVM()
      .setLabelCol(labelCol)
      .setFeatureCol(featureCols)
      .setPartitionNum(partitionNum)
      .setSampleRate(sampleRate)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatch)
      .setMaxIter(maxIter)
      .setModelPath(modelPath)
      .setPMLLModelPath(PMMLmodelPath)
      .setSep(sep)


    svm.process(actionType, input, modelPath, testSetPath, output)
    LogUtils.logTime(s"${this.getClass.getSimpleName} finished.")
  }
}



