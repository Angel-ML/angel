package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.classification.naivebayes.NaiveBayes
import com.tencent.angel.spark.ml.util._
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
/**
 *
 * Naive Bayes分类器
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
 * modelType   model type: multinomial or bernoulli
 *
 * ----system parameter-----
 * mode          local or yarn-cluster
 * actionType    train/predict
 * modelPath     model HDFS path. train阶段, 模型保存到该路径, predict阶段, 从该路径load模型
 * testSet       testSet path for evaluate trained model. if null, then skip test step
 * output        预测的输出路径, 在train阶段, output == null
 */

object NaiveBayesCluster  {

  val MULTI_NOMIAL_MODE = "multinomial"
  val BERNOULLI_MODE  = "bernoulli"
  val MODEL_TYPE_FLAG = "modelType"
  val NUM_CLASSES = "numClasses"

  def main(args: Array[String]): Unit = {
    LogUtils.logTime(s"Start Naive Bayes Processing...")

    val params = coreArgsUtil.parse(args)

    val mode = params.getOrElse(ParamKeys.MODE, "yarn-cluster")
    val input = params.getOrElse(ParamKeys.INPUT, null)
    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, MAX_INT_STR).toInt
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse(ParamKeys.SAMPLE_RATE, "1.0").toDouble
    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "4").toInt
    val modelType = params.getOrElse(this.MODEL_TYPE_FLAG, "multinomial")
    val numClasses = params.getOrElse(NUM_CLASSES, "2").toInt
    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, "train")

    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)
    val testSetPath = params.getOrElse(ParamKeys.TEST_SET, null)
    val output = params.getOrElse(ParamKeys.OUTPUT, null)
    val PMMLmodelPath = params.getOrElse("PMMLmodelPath", null)
    val outputType = params.getOrElse("outputType", "merge")

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }


    // spark configure
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val nb = new NaiveBayes()
      .setLabelCol(labelCol)
      .setFeatureCol(featureCols)
      .setPartitionNum(partitionNum)
      .setSampleRate(sampleRate)
      .setModelType(modelType)
      .setModelPath(modelPath)
      .setPMLLModelPath(PMMLmodelPath)
      .setSep(sep)
      .setOutputType(outputType)
      .setNumClasses(numClasses)


    nb.process(actionType, input, modelPath, testSetPath, output)
    LogUtils.logTime(s"${this.getClass.getSimpleName} finished.")
  }
}
