package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.util._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.jpmml.model.MetroJAXBUtil
import org.jpmml.sparkml.PMMLBuilder
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import scala.util.Random

/**
 * Created by isakjiang on 2020/12/30.
 */

class RandomForestRegressionWithMLCluster extends Learner {
  private var labelCol = 0
  private var featureColExpr: String = null
  private var partitionNum = 10
  private var sampleRate = 1.0
  private var impurity: String = "Entropy"
  private var maxDepth = 10
  private var numClasses = 2
  private var minInstancePerNode = 1
  private var minInfoGain = 0.0
  private var maxBins = 32
  private var numTrees = 10
  private var featureSubsetStrategy = "auto"
  private var subsamplingRate = 1.0
  private var modelPath: String = _
  private var sep = " "
  val TRAIN_PREFIX = "train for"
  val TEST_PREFIX = "test for"
  val BLANK = " "
  private var PMMLmodelPath: String = _

  def setLabelCol(col: Int): this.type = {
    this.labelCol = col
    this
  }

  def setPMLLModelPath(PMMLmPath: String): this.type = {
    this.PMMLmodelPath = PMMLmPath
    this
  }

  def setSep(sep: String): this.type = {
    this.sep = sep
    this
  }

  //new feature selection methods
  def setFeatureCol(cols: String): this.type = {
    this.featureColExpr = cols
    this
  }

  def setPartitionNum(num: Int): this.type = {
    this.partitionNum = num
    this
  }

  def setSampleRate(rate: Double): this.type = {
    this.sampleRate = rate
    this
  }

  def setImpurity(impurity: String): this.type = {
    this.impurity = impurity
    this
  }

  def setMaxDepth(maxDepth: Int): this.type = {
    this.maxDepth = maxDepth
    this
  }

  def setNumClasses(numClasses: Int): this.type = {
    this.numClasses = numClasses
    this
  }

  def setMinInstancePerNode(minInstancePerNode: Int): this.type = {
    this.minInstancePerNode = minInstancePerNode
    this
  }

  def setMinInfoGain(minInfoGain: Double): this.type = {
    this.minInfoGain = minInfoGain
    this
  }

  def setMaxBins(maxBins: Int): this.type = {
    this.maxBins = maxBins
    this
  }

  def setNumTrees(numTrees: Int): this.type = {
    this.numTrees = numTrees
    this
  }

  def setSubsamplingRate(subsamplingRate: Double): this.type = {
    this.subsamplingRate = subsamplingRate
    this
  }

  def setFeatureSubsetStrategy(featureSubsetStrategy: String): this.type = {
    this.featureSubsetStrategy = featureSubsetStrategy
    this
  }

  def setModelPath(modelPath: String): this.type = {
    this.modelPath = modelPath
    this
  }

  override def train(input: String, testSet: String): Model = {

    val sc = SparkContext.getOrCreate()
    var trainingDFtable = DataLoader.loadTableWithLabel(sc, input, partitionNum, sampleRate, labelCol, featureColExpr, sep)
    LogUtils.logTime(s"Train data size: ${trainingDFtable.count()}")

    val featuresCols = trainingDFtable.columns

    trainingDFtable = trainingDFtable.select(featuresCols.map(name => col(name).cast(DoubleType)): _*)
      .withColumnRenamed(featuresCols(0), "label")

    val assembler: VectorAssembler = new VectorAssembler().setInputCols(featuresCols.drop(1)).setOutputCol("feature")
    val jModel = new MLModel().initSummary(modelPath)

    val seed = (new Random).nextInt()

    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("feature")
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minInstancePerNode)
      .setNumTrees(numTrees)
      .setSeed(seed)
      .setSubsamplingRate(subsamplingRate)
      .setImpurity(impurity)

    val pipeline = new Pipeline().setStages(Array(assembler, rf))

    val pipelineModel = pipeline.fit(trainingDFtable)
    println("success fit......")

    val pmml = new PMMLBuilder(trainingDFtable.schema, pipelineModel).build()
    if (PMMLmodelPath != "" && PMMLmodelPath != null) {
      val baos = new ByteArrayOutputStream()
      MetroJAXBUtil.marshalPMML(pmml, baos)
      val ss = trainingDFtable.sparkSession
      import ss.implicits._
      val writer = ss.createDataset(Seq(baos.toString)).write
      writer.mode(SaveMode.Overwrite)
      val savePath = new Path(PMMLmodelPath)
      writer.text(savePath.toString)
    }

    val lrModel = pipelineModel.stages(1).asInstanceOf[RandomForestRegressionModel]
    jModel.initModel(lrModel)
    jModel
  }


  override def predict(input: String, output: String, model: Model): Unit = {
    val sc = SparkContext.getOrCreate()
    val predictData = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureColExpr, false, sep)
    //predictData.show(100)
    LogUtils.logTime(s"Predict data size: ${predictData.count()}")

    val outputDF = model.predict(predictData).drop("feature")
    DataSaver.save(outputDF, output, sep)
  }


  override def loadModel(modelPath: String): Model = {
    MLModel.loadFrom(modelPath, RandomForestRegressionModel)
  }
}


object RandomForestRegressionWithMLCluster {

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

    // data parameters
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val partitionNum = params.getOrElse("partitionNum", "4").toInt
    val output = params.getOrElse("output", null)
    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, MAX_INT_STR).toInt
    val numClasses = params.getOrElse(NUM_CLASSES, "2").toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    // model parameters
    val numTrees = params.getOrElse(NUM_TREES, "10").toInt
    val featureSubsetStrategy = params.getOrElse(FEATURE_SUBSET_STRATEGY, "auto")
    val subsamplingRate = params.getOrElse(SUB_SAMPLING_RATE, "1.0").toDouble

    // decision tree parameters
    val maxDepth = params.getOrElse(MAX_DEPTH, "10").toInt
    val maxBins = params.getOrElse(MAX_BINS, "32").toInt

    val impurity = params.getOrElse(IMPURITY, "Variance")

    // hide
    val minInstancesPerNode = params.getOrElse(MIN_INSTANCES_PER_NODE, "1").toInt
    val minInfoGain = params.getOrElse(MIN_INFO_GAIN, "0.0").toDouble

    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, "train")
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)
    val PMMLmodelPath = params.getOrElse("PMMLmodelPath", null)
    val testSetPath = params.getOrElse(ParamKeys.TEST_SET, null)

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rf = new RandomForestRegressionWithMLCluster()
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
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setNumTrees(numTrees)
      .setSubsamplingRate(subsamplingRate)
      .setModelPath(modelPath)
      .setPMLLModelPath(PMMLmodelPath)
      .setSep(sep)

    rf.process(actionType, input, modelPath, testSetPath, output)
  }

}
