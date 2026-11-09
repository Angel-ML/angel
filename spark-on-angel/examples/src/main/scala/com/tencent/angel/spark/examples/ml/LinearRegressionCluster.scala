package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression => MLLiR, LinearRegressionModel => MLLiRModel}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.jpmml.model.MetroJAXBUtil
import org.jpmml.sparkml.PMMLBuilder

/**
 * Created by isakjiang on 2020/12/30.
 */
class LinearRegressionCluster extends Learner {
  private var labelCol = 0
  private var featureColExpr: String = null
  private var partitionNum = 100
  private var sampleRate = 1.0
  private var regParam = 0.1
  private var elasticNet = 1.0
  private var sep = " "
  private var maxIter = 50
  private var modelPath: String = _
  private var PMMLmodelPath: String = _

  def setSep(sep: String): this.type = {
    this.sep = sep
    this
  }

  def setPMLLModelPath(PMMLmPath: String): this.type = {
    this.PMMLmodelPath = PMMLmPath
    this
  }

  def setLabelCol(labelCol: Int): this.type = {
    this.labelCol = labelCol
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

  def setElasticNet(elasticNet: Double): this.type = {
    this.elasticNet = elasticNet
    this
  }

  def setMaxIter(maxIter: Int): this.type = {
    this.maxIter = maxIter
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

    // init metrics model
    val lirSummaryModel = new MLModel().initSummary(modelPath)

    val lr = new MLLiR()
      .setFeaturesCol(DFStruct.FEATURE)
      .setLabelCol(DFStruct.LABEL)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNet)
      .setMaxIter(maxIter)

    val pipeline = new Pipeline().setStages(Array(assembler, lr))

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

    val lrModel = pipelineModel.stages(1).asInstanceOf[MLLiRModel]

    LogUtils.logTime(s"[LinearRegressionModel] coefficients length: ${lrModel.coefficients.size}")
    LogUtils.logTime(s"[LinearRegressionModel] coefficients: ${lrModel.coefficients.toString}")
    LogUtils.logTime(s"[LinearRegressionModel] intercept: ${lrModel.intercept}")

    val lirModel = new MLModel(lrModel)
    lirSummaryModel.initModel(lrModel)

    if (lrModel.hasSummary) {
      val historyLoss = lrModel.summary.objectiveHistory.zipWithIndex
        .map { case (loss, id) => (id - 1.0, loss) }
      lirSummaryModel.metricsSummary.logMetrics("loss", historyLoss)
    }

    if (testSet != null) {
      val testSetDF = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, featureColExpr, labelCol, false, sep)
      //TODO: Regression evalation metrics
    }

    lirModel
  }

  override def predict(input: String, output: String, model: Model) {
    val sc = SparkContext.getOrCreate()
    val predictData = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureColExpr,false,sep)
    LogUtils.logTime(s"Predict data size: ${predictData.count()}")

    val outputDF = model.predict(predictData).drop(DFStruct.FEATURE)
    DataSaver.save(outputDF, output,sep)
  }

  override def loadModel(modelPath: String): Model = {
    MLModel.loadFrom(modelPath, MLLiRModel)
  }
}


/**
 *
 * Linear Regression 线性回归
 *
 * 参数说明:
 *
 * -----input parameter-----
 * input         input path
 * labelCol      标签列(从0开始计数), default: 0
 * beginCol      特征的起始列(从0开始计数, 包含当前列), default: 1
 * endCol        特征的终止列(从0开始计数, 包含当前列); default: length - 1
 * partitionNUm   partition数目, 并行数
 * sampleRate    采样率. 默认:1.0 [optional]
 *
 * ----algorithm parameter----
 * regParam      正则项系数
 * elasticNet    范围:0~1.0, 0表示L2正则, 1表示L1正则. 0~1表示L1和L2的混合
 * maxIter       最大迭代次数
 *
 * ----system paramater-----
 * mode          local or yarn-cluster
 * actionType    train/predict
 * modelPath     model HDFS path. train阶段, 模型保存到该路径, predict阶段, 从该路径load模型
 * testSet       testSet path for evaluate trained model. if null, then skip test step
 * output        预测的输出路径, 在train阶段, output == null
 */

object LinearRegressionCluster {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime("Start Linear Regression Procession...")

    val params = coreArgsUtil.parse(args)

    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val partitionNum = params.getOrElse("partitionNum", "4").toInt
    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, MAX_INT_STR).toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }


    // model parameters
    val regParam = params.getOrElse(ParamKeys.REG_PARAM, "0.0").toDouble
    val elasticNet = params.getOrElse(ParamKeys.ELASTIC_NET, "0.0").toDouble
    val maxIter = params.getOrElse(ParamKeys.MAX_ITER, "100").toInt

    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, null)
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)
    val PMMLmodelPath = params.getOrElse("PMMLmodelPath", null)
    val testSetPath = params.getOrElse(ParamKeys.TEST_SET, null)
    val output = params.getOrElse(ParamKeys.OUTPUT, null)

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lir = new LinearRegressionCluster()
      .setLabelCol(labelCol)
      .setFeatureCol(featureCols)
      .setPartitionNum(partitionNum)
      .setSampleRate(sampleRate)
      .setRegParam(regParam)
      .setElasticNet(elasticNet)
      .setMaxIter(maxIter)
      .setModelPath(modelPath)
      .setPMLLModelPath(PMMLmodelPath)
      .setSep(sep)
    lir.process(actionType, input, modelPath, testSetPath, output)

    LogUtils.logTime(s"${this.getClass.getSimpleName} finished.")
  }
}


