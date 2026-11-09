package com.tencent.angel.spark.ml.classification.lr

import com.tencent.angel.spark.ml.util._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.classification.{LogisticRegression => MLLR, LogisticRegressionModel => MLLRModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.jpmml.model.MetroJAXBUtil
import org.jpmml.sparkml.PMMLBuilder


class LogisticRegressionLocal extends Learner {
  private var labelCol = 0
  private var featureColExpr: String = null
  private var partitionNum = 100
  private var sampleRate = 1.0
  private var regParam = 0.1
  private var elasticNet = 1.0
  private var maxIter = 50
  private var sep = " "
  private var modelPath: String = _
  private var PMMLmodelPath: String = _
  private var outputType = "merge"

  def setOutputType(outputType: String): this.type = {
    this.outputType = outputType
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

  def setModelPath(mPath: String): this.type = {
    this.modelPath = mPath
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

    //val output = assembler.transform(trainingDFtable).drop(featuresCols.drop(1):_*)
    //output.show(5)

    val jModel = new MLModel().initSummary(modelPath)

    jModel.modelMeta.logMeta("instance count", trainingDFtable.count())
    jModel.modelMeta.logMeta("feature count", featuresCols.length)

    // train LR model
    val lr = new MLLR()
      .setFeaturesCol("feature")
      .setLabelCol("label")
      .setRegParam(regParam)
      .setElasticNetParam(elasticNet)
      .setMaxIter(maxIter)
      .setPredictionCol("predictionCol")
      .setProbabilityCol("prob")
      .setRawPredictionCol("")


    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    val pipelineModel = pipeline.fit(trainingDFtable)
    println("success fit......")
    val pmml = new PMMLBuilder(trainingDFtable.schema, pipelineModel).build()

    if (PMMLmodelPath != "" && PMMLmodelPath != null) {
      /*
      val fis: FileOutputStream = new FileOutputStream(PMMLmodelPath)
      val fout: StreamResult = new StreamResult(fis)
      JAXBUtil.marshalPMML(pmml, fout)
      println("pmml success......")

       */

      val baos = new ByteArrayOutputStream()
      MetroJAXBUtil.marshalPMML(pmml, baos)
      val ss = trainingDFtable.sparkSession
      import ss.implicits._
      val writer = ss.createDataset(Seq(baos.toString)).write
      writer.mode(SaveMode.Overwrite)
      val savePath = new Path(PMMLmodelPath)
      writer.text(savePath.toString)


    }


    val lrModel = pipelineModel.stages(1).asInstanceOf[MLLRModel]
    println(lrModel)

    jModel.initModel(lrModel)
    // save loss history
    LogUtils.logTime(s"train model finish...")
    LogUtils.logTime(s"[LogisticRegressionModel] coefficients length: ${lrModel.coefficientMatrix.numCols}")
    LogUtils.logTime(s"[LogisticRegressionModel] coefficients: ${lrModel.coefficients.toString}")
    LogUtils.logTime(s"[LogisticRegressionModel] intercept: ${lrModel.intercept}")

    if (lrModel.hasSummary) {
      val historyLoss = lrModel.summary.objectiveHistory.zipWithIndex
        .map { case (loss, id) => (id - 1.0, loss) }
      jModel.metricsSummary.logMetrics("loss", historyLoss)
      LogUtils.logTime("history loss")
      LogUtils.logTime(historyLoss.mkString(","))

    }
    // evaluate model
    if (testSet != null) {
      val auc = evaluate(testSet, jModel)
      jModel.modelMeta.logMeta("test AUC", auc)
    }

    jModel
  }

  override def predict(input: String, output: String, model: Model) {
    val sc = SparkContext.getOrCreate()
    val predictData = DataLoader.loadUnlabeled(sc, input, partitionNum, 1.0, featureColExpr, false, sep)
    LogUtils.logTime(s"Predict data size: ${predictData.count()}")

    val outputDF = model.predict(predictData).drop("feature")

    if (outputType == "merge") {
      DataSaver.save(outputDF, output, sep)
    }
    else {
      val vecToArray = udf((xs: linalg.Vector) => xs.toArray)
      var outputFDFinal = outputDF.withColumn("arrayCol", vecToArray(col("prob")))
        .withColumn("proCol0", col("arrayCol").getItem(0))
        .withColumn("proCol1", col("arrayCol").getItem(1))
        .drop("arrayCol").drop("prob")

      val cols = outputFDFinal.columns
      val colsLength = cols.length

      val colsNew = cols.slice(0, colsLength - 3) ++ Array(cols(colsLength - 2), cols(colsLength - 1), cols(colsLength - 3))
      outputFDFinal = outputFDFinal.select(colsNew.head, colsNew.tail: _*)

      println(cols.mkString(","))
      println(colsNew.mkString(","))
      println(outputFDFinal.schema)
      outputFDFinal.show(5)

      DataSaver.save(outputFDFinal, output, sep)
    }
  }

  def evaluate(testSet: String, trainedModel: Model): Double = {
    val sc = SparkContext.getOrCreate()

    val testSetDF = DataLoader.loadlabeled(sc, testSet, partitionNum, 1.0, featureColExpr, labelCol, false, sep)
    val binEvaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .setLabelCol("label")
      .setRawPredictionCol("prob")
    val auc = trainedModel.evaluate(testSetDF, binEvaluator)
    LogUtils.logTime(s"[LogisticRegression] test set AUC: $auc")
    auc
  }

  override def loadModel(modelPath: String): Model = {
    MLModel.loadFrom(modelPath, MLLRModel)
  }
}

/**
 *
 * Logistic Regression 逻辑回归
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
 * elasticNet    范围:0~1.0, 0表示L2正则, 1表示L1正则. 0~1表示L1和L2的混合
 * maxIter       最大迭代次数
 *
 * ----system parameter-----
 * mode          local or yarn-cluster
 * actionType    train/predict
 * modelPath     model HDFS path. train阶段, 模型保存到该路径, predict阶段, 从该路径load模型
 * testSet       testSet path for evaluate trained model. if null, then skip test step
 * output        预测的输出路径, 在train阶段, output == null
 */

object LogisticRegressionLocal {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime("Start Logistic Regression Processing...")

    val mode = "local"
    val partitionNum = 1
    val sampleRate = 1


    val input = "data/mlTestData/data_test_libsvm.txt" //xxxx.txt,poker_hand.csv
    val labelCol = 0
    val featureCols = "1-20"
    val sep = " "


    /*
    val input = "data/mlTestData/iris_two_class.csv" //xxxx.txt,poker_hand.csv
    val labelCol = 4
    val featureCols = "0-3"
    val sep = ","
     */
    val regParam = 0
    val elasticNet = 1
    val maxIter = 10

    val actionType = "predict" //"predict"//"train"
    val modelPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/modeLlout"
    val PMMLmodelPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/pmll3"
    val testSetPath = input
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outLR"
    val outputType = "merge" //separate，merge


    // spark configure
    // SparkContext must be create here.
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    val lr = new LogisticRegressionLocal()
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
      .setOutputType(outputType)

    lr.process(actionType, input, modelPath, testSetPath, output)
    LogUtils.logTime(s"${this.getClass.getSimpleName} finished.")

    sc.stop()
  }
}