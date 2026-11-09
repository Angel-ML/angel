package com.tencent.angel.spark.ml.classification.svm

import com.tencent.angel.spark.ml.util._
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.{Vector => mlVector}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.{Vectors => mllibVectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by isakjiang on 2022-04-25.
 */

class SVM extends Learner {
  private var labelCol = 0
  private var featureColExpr: String = null
  private var partitionNum = 10
  private var sampleRate = 1.0
  private var regParam = 0.01
  private var stepSize = 1.0
  private var miniBatchFraction = 1.0
  private var maxIter = 50
  private var modelPath: String = _
  val TRAIN_PREFIX = "train for"
  val TEST_PREFIX = "test for"
  val BLANK = " "
  private var sep = " "
  private var PMMLmodelPath: String = _


  def setPMLLModelPath(PMMLmPath: String): this.type = {
    this.PMMLmodelPath = PMMLmPath
    this
  }

  def setSep(sep: String): this.type = {
    this.sep = sep
    this
  }


  def setLabelCol(col: Int): this.type = {
    this.labelCol = col
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

  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  def setStepSize(stepSize: Double): this.type = {
    this.stepSize = stepSize
    this
  }

  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  def setMaxIter(num: Int): this.type = {
    this.maxIter = num
    this
  }

  def setModelPath(modelPath: String): this.type = {
    this.modelPath = modelPath
    this
  }

  override def train(input: String, testSet: String): Model = {
    val sc = SparkContext.getOrCreate()
    val trainDF = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, featureColExpr, labelCol, false, sep)
    LogUtils.logTime(s"train data size: ${trainDF.count()}")

    val trainRDD = trainDF.select("label", "feature").rdd
      .map { case Row(label: Double, feature: mlVector) => LabeledPoint(label, mllibVectors.fromML(feature)) }
    LogUtils.logTime(s"train data size: ${trainDF.count()}")

    val mlModel = SVMWithSGD.train(trainRDD, maxIter, stepSize, regParam, miniBatchFraction)


    LogUtils.logTime(s"Save model weight size: ${mlModel.weights.size}")
    LogUtils.logTime(s"[SVMModel] weight: ${mlModel.weights.toString}")
    LogUtils.logTime(s"[SVMModel] intercept: ${mlModel.intercept}")


    if (PMMLmodelPath != "" && PMMLmodelPath != null) {
      val sc = SparkContext.getOrCreate()
      val fsPath = new Path(PMMLmodelPath)
      val fs = fsPath.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(fsPath)) {
        fs.delete(fsPath, true)
      }
      mlModel.toPMML(sc, PMMLmodelPath)
    }

    val svmModel = new SVMModelDefine(mlModel)
    svmModel.initSummary(modelPath)

    // model meta from train set and test set
    val svmModelMeta = new ModelMeta(svmModel)

    svmModelMeta.setMulClaModelMeta(
      sc,
      input,
      partitionNum,
      1.0,
      featureColExpr,
      labelCol,
      TRAIN_PREFIX + BLANK,
      sep)

    if (testSet != null) {
      svmModelMeta.setMulClaModelMeta(
        sc,
        testSet,
        partitionNum,
        1.0,
        featureColExpr,
        labelCol,
        TEST_PREFIX + BLANK,
        sep)
    }

    svmModel
  }

  override def predict(input: String, output: String, model: Model): Unit = {
    val sc = SparkContext.getOrCreate()
    val predictData = DataLoader.loadUnlabeled(sc, input, partitionNum, 1.0, featureColExpr, false, sep)
    LogUtils.logTime(s"Predict data size: ${predictData.count()}")

    val outputDF = model.predict(predictData).drop(DFStruct.FEATURE)
    DataSaver.save(outputDF, output, sep)
  }

  override def loadModel(modelPath: String): Model = {
    SVMModelDefineModel.loadFrom(Model.getModelDir(modelPath), SVMModel)
  }

  class SVMModelDefine(model: Saveable) extends MLlibModel(model) {
    override def predict(input: DataFrame): DataFrame = {
      val mllibModel = model.asInstanceOf[SVMModel]

      val predictUDF = udf { (feature: Any) =>
        mllibModel.predict(mllibVectors.fromML(feature.asInstanceOf[mlVector]))
      }
      input.withColumn("prob", predictUDF(col("feature")))
    }
  }

  object SVMModelDefineModel {
    def loadFrom[T <: Saveable](modelPath: String, loader: Loader[T]) = {
      val model = loader.load(SparkContext.getOrCreate(), modelPath).asInstanceOf[Saveable]
      new SVMModelDefine(model)
    }
  }


}


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

object SVMLocal {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime("Start SVM Processing...")
    val mode = "local"
    val partitionNum = 1
    val sampleRate = 1


    val input = "data/mlTestData/iris_two_class.csv"
    val labelCol = 4
    val featureCols = "0-3"
    val sep = ","
    val actionType = "predict" //"predict"//"train"
    val modelPath = "data/output/modeLloutSVM"
    val PMMLmodelPath = "data/output/pmll3SVM"
    val testSetPath = input
    val output = "data/output/outLR"


    val regParam = 0
    val maxIter = 10
    val stepSize = 1
    val miniBatch = 1.0

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
