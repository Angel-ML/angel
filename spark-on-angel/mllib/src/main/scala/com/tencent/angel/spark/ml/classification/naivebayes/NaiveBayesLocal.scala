package com.tencent.angel.spark.ml.classification.naivebayes

import com.tencent.angel.spark.ml.util._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.ml.classification.{NaiveBayes => MLNB, NaiveBayesModel => MLNBMODEL}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.jpmml.model.MetroJAXBUtil
import org.jpmml.sparkml.PMMLBuilder

/**
  * Created by isakjiang on 2022-04-25.
  */

class NaiveBayes extends Learner {
  private var labelCol = 0
  private var featureColExpr: String = null
  private var partitionNum = 100
  private var sampleRate = 1.0
  private var modelType = "multinomial"
  private var modelPath: String = _
  val TRAIN_PREFIX = "train for"
  val TEST_PREFIX = "test for"
  val BLANK = " "
  private var sep = " "
  private var PMMLmodelPath: String = _
  private var outputType = "merge"
  private var numClasses = 2

  def setOutputType(outputType: String): this.type = {
    this.outputType = outputType
    this
  }

  def setNumClasses(numClasses: Int): this.type = {
    this.numClasses = numClasses
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

  def setSep(sep: String): this.type = {
    this.sep = sep
    this
  }

  def setModelType(mType: String): this.type = {
    this.modelType = mType
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


    val nbModelForTrain = new MLNB()
      .setFeaturesCol(DFStruct.FEATURE)
      .setLabelCol(DFStruct.LABEL)
      .setRawPredictionCol("")
      .setProbabilityCol(DFStruct.PROB)
      .setModelType(modelType)


    val pipeline = new Pipeline().setStages(Array(assembler, nbModelForTrain))

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

    val mlModel = pipelineModel.stages(1).asInstanceOf[MLNBMODEL]
    LogUtils.logTime(s"[NaiveBayes] train finish.")
    LogUtils.logTime(s"[NaiveBayes] pi(log of class priors: C(Number of class) * 1): ${mlModel.pi.toArray.mkString(" ")}")

    val theta = mlModel.theta
    val thetaString = (0 until theta.numRows).map { row =>
      (0 until theta.numCols).map(col => theta(row, col)).mkString(" ")
    }.mkString("\n")
    LogUtils.logTime(s"[NaiveBayes] theta(log of class conditional probabilities:" +
      s" C(Number of class) * N(Number of features)): \n $thetaString")

    val nbModel = new MLModel(mlModel)

    val nbModelPreVal = new NBModel(mlModel)
    val nbModelSummary = nbModelPreVal.initSummary(modelPath)

    // model meta from train set and test set
    val nbModelMeta = new ModelMeta(nbModelSummary)

      nbModelMeta.setMulClaModelMeta(
        sc,
        input,
        partitionNum,
        1.0,
        featureColExpr,
        labelCol,
        TRAIN_PREFIX + BLANK,
        sep)

      if (testSet != null) {
        nbModelMeta.setMulClaModelMeta(
          sc,
          testSet,
          partitionNum,
          1.0,
          featureColExpr,
          labelCol,
          TEST_PREFIX + BLANK,
          sep)
      }

    nbModel

  }
  override def predict(input: String, output: String, model: Model) {
    val sc = SparkContext.getOrCreate()
    val predictData = DataLoader.loadUnlabeled(sc, input, partitionNum, 1.0, featureColExpr,false,sep)
    LogUtils.logTime(s"Predict data size: ${predictData.count()}")

    val outputDF = model.predict(predictData).drop(DFStruct.FEATURE)
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
    MLModel.loadFrom(modelPath, MLNBMODEL)
  }
}

class NBModel(model: MLWritable) extends MLModel(model) {

  override def predict(input: DataFrame): DataFrame = {

    val nbModel = model.asInstanceOf[MLNBMODEL]
    val predictDF = nbModel.transform(input)
    val preColName = nbModel.getPredictionCol

    predictDF.select(col(preColName).as(DFStruct.PROB), col(DFStruct.LABEL))
  }
}


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

object NaiveBayesLocal  {

  val MULTI_NOMIAL_MODE = "multinomial"
  val BERNOULLI_MODE  = "bernoulli"
  val MODEL_TYPE_FLAG = "modelType"

  def main(args: Array[String]): Unit = {
    LogUtils.logTime(s"Start Naive Bayes Processing...")
    val mode = "local"
    val partitionNum = 1
    val sampleRate = 1


    val input = "data/mlTestData/Iris.csv"
    val labelCol = 4
    val featureCols = "0-3"
    val sep = ","
    val actionType = "predict" //"predict"//"train"
    val modelPath = "data/output/modeLlout"
    val PMMLmodelPath = "data/output/pmll3"
    val testSetPath = input
    val output = "data/output/outLR"
    val outputType = "separate" //separate，merge
    val modelType ="multinomial"//multinomial,bernoulli
    val numClasses = 3

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
