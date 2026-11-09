package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.classification.tree.DecisionTreeClassificationWithML
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{MAX_INT_STR, ParamKeys}

/**
 * Created by isakjiang on 2022/04/25.
 */


object DecisionTreeClassificationWithMLCluster {

  val NUM_CLASSES = "numClasses"
  val NUM_TREES = "numTrees"
  val FEATURE_SUBSET_STRATEGY = "featureSubsetStrategy"
  val SUB_SAMPLING_RATE = "subSamplingRate"

  val MAX_DEPTH = "maxDepth"
  val MAX_BINS = "maxBins"
  val MAX_CATEGORIES = "maxCategories"
  val MIN_INSTANCES_PER_NODE = "minInstancesPerNode"
  val IMPURITY = "impurity"
  val MIN_INFO_GAIN = "minInfoGain"


  /**
   *
   * DecisionTree 决策树
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
   * numClasses    标签数，默认:2
   *
   * ----algorithm parameter----
   * numTrees                  森林规模，决策树棵树
   * featureSubsetStrategy     特征抽样规则，可选为"auto","sqrt","log2"
   * subsamplingRate           构建决策数的样本比例
   * maxDepth                  最大深度
   * maxBins                   最大分支数
   * impurity                  信息增益计算方法，可选为"entropy"或"gini"
   * minInstancesPerNode       每个叶子节点至少有多少个样本
   * minInfoGain               节点分裂至少要得到的信息增益
   * ----system parameter-----
   * mode          local or yarn-cluster
   * actionType    train/predict
   * modelPath     model HDFS path. train阶段, 模型保存到该路径, predict阶段, 从该路径load模型
   * testSet       testSet path for evaluate trained model. if null, then skip test step
   * output        预测的输出路径, 在train阶段, output == null
   */

  def main(args: Array[String]): Unit = {

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
    val outputType = params.getOrElse("outputType", "merge")


    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    // hide
    val minInstancesPerNode = params.getOrElse(MIN_INSTANCES_PER_NODE, "1").toInt
    val minInfoGain = params.getOrElse(MIN_INFO_GAIN, "0.0").toDouble

    // algorithm parameters
    val maxDepth = params.getOrElse(MAX_DEPTH, "10").toInt
    val maxBins = params.getOrElse(MAX_BINS, "32").toInt
    val impurity = params.getOrElse(IMPURITY, "gini")
    val numClasses = params.getOrElse(NUM_CLASSES, "2").toInt



    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rf = new DecisionTreeClassificationWithML()
      .setLabelCol(labelCol)
      .setFeatureCol(featureCols)
      .setPartitionNum(partitionNum)
      .setImpurity(impurity)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setMinInstancePerNode(minInstancesPerNode)
      .setNumClasses(numClasses)
      .setMaxBins(maxBins)
      .setSampleRate(sampleRate)
      .setModelPath(modelPath)
      .setPMLLModelPath(PMMLmodelPath)
      .setSep(sep)
      .setOutputType(outputType)


    rf.process(actionType, input, modelPath, testSetPath, output)
  }

}