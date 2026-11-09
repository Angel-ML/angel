package com.tencent.angel.spark.ml.classification.tree

import com.tencent.angel.spark.ml.util._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.jpmml.model.MetroJAXBUtil
import org.jpmml.sparkml.PMMLBuilder

import scala.util.Random

/**
 * Created by isakjiang on 2022/04/25.
 */

class DecisionTreeClassificationWithML extends Learner {
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
  private var modelPath: String = _
  private var sep = " "
  val TRAIN_PREFIX = "train for"
  val TEST_PREFIX = "test for"
  val BLANK = " "
  private var PMMLmodelPath: String = _
  private var outputType = "merge"

  def setOutputType(outputType: String): this.type = {
    this.outputType = outputType
    this
  }

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

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("feature")
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minInstancePerNode)
      .setSeed(seed)
      .setImpurity(impurity)
      .setRawPredictionCol("")
      .setProbabilityCol(DFStruct.PROB)

    val pipeline = new Pipeline().setStages(Array(assembler, dt))

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

    val dtModel = pipelineModel.stages(1).asInstanceOf[DecisionTreeClassificationModel]
    jModel.initModel(dtModel)

    val decisionTreeModel = new DTModel(dtModel)
    // model meta from train set and test set
    val dtModelSummary = decisionTreeModel.initSummary(modelPath)

    val dtModelMeta = new ModelMeta(dtModelSummary)

    dtModelMeta.setMulClaModelMeta(
      sc,
      input,
      partitionNum,
      1.0,
      featureColExpr,
      labelCol,
      TRAIN_PREFIX + BLANK,
      sep)

    if (testSet != null) {
      dtModelMeta.setMulClaModelMeta(
        sc,
        testSet,
        partitionNum,
        1.0,
        featureColExpr,
        labelCol,
        TEST_PREFIX + BLANK,
        sep)
    }
    jModel
  }


  override def predict(input: String, output: String, model: Model): Unit = {
    val sc = SparkContext.getOrCreate()
    val predictData = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureColExpr, false, sep)
    LogUtils.logTime(s"Predict data size: ${predictData.count()}")

    val outputDF = model.predict(predictData).drop("feature")

    if (outputType == "merge") {
      DataSaver.save(outputDF, output, sep)
    }
    else {
      val vecToArray = udf((xs: linalg.Vector) => xs.toArray)
      var outputFDFinal = outputDF.withColumn("arrayCol", vecToArray(col("prob")))

      Array.range(0, numClasses).foreach { i =>
        outputFDFinal = outputFDFinal.withColumn(s"proCol$i", col("arrayCol").getItem(i))
      }

      outputFDFinal = outputFDFinal.withColumn("predictionEnd", col("prediction"))
      outputFDFinal = outputFDFinal.drop(s"arrayCol").drop("prob").drop("prediction")
      DataSaver.save(outputFDFinal, output, sep)

    }
  }


  override def loadModel(modelPath: String): Model = {
    MLModel.loadFrom(modelPath, DecisionTreeClassificationModel)
  }
}

class DTModel(model: MLWritable) extends MLModel(model) {
  override def predict(input: DataFrame): DataFrame = {

    val dtModel = model.asInstanceOf[DecisionTreeClassificationModel]
    val predictDF = dtModel.transform(input)
    val preColName = dtModel.getPredictionCol

    predictDF.select(col(preColName).as(DFStruct.PROB), col(DFStruct.LABEL))
  }
}


object DecisionTreeClassificationWithML {

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

    val mode = "local"

    // data parameters
    val input = "data/mlTestData/Iris.csv" //regression_data_test.txt"//xxxx.txt,poker_hand.csv
    val labelCol = 4
    val partitionNum = 1
    val sampleRate = 1
    val numClasses = 3
    val featureCols = "0-3"
    val sep = ","

    // decision tree parameters
    val maxDepth = 10
    val maxBins = 32
    val impurity = "gini" //Entropy,Gini

    // hide
    val minInstancesPerNode = 1
    val minInfoGain = 0

    val modelPath = "data/output/modeLlout"
    val PMMLmodelPath = "data/output/pmll3"
    val testSetPath = input
    val output = "data/output/outLR"
    val actionType = "predict" //"predict"//"train"
    val outputType = "separate" //separate，merge


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