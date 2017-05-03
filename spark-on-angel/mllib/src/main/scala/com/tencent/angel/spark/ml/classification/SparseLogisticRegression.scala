package com.tencent.angel.spark.ml.classification

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.ml.common.{Learner, LogisticGradient, Model}
import com.tencent.angel.spark.ml.optim.ADMM
import com.tencent.angel.spark.ml.util._

/**
 * Sparse Logistic Regression is good at training sparse and high dimension Logistic Regression
 * Model with ADMM(Alternating Direction Method of Multipliers) optimizer.
 *
 * This algorithm only supports binary classification task right now. And the feature of train
 * data must be one hot feature(each dimension of feature must be either 0 or 1). We have train
 * 100 million dimension level model in 2~3 hours.
 *
 */
class SparseLogisticRegression extends Learner {
  private var partitionNum: Int = _
  private var sampleRate: Double = _
  private var regParam = 0.0
  private var maxIter = 15
  private var numSubModels = 100
  private var rho = 0.01

  /**
   * set parallel number in spark
   */
  def setPartitionNum(num: Int): this.type = {
    partitionNum = num
    this
  }

  /**
   * set sample rate of data for train. Default 1.0
   */
  def setSampleRate(fraction: Double): this.type = {
    sampleRate = fraction
    this
  }

  /**
   * set the regularization parameter. Default 0.0
   */
  def setRegParam(reg: Double): this.type = {
    regParam = reg
    this
  }

  /**
   * set maximum number of iteration. Default 15
   */
  def setMaxIter(num: Int): this.type = {
    maxIter = num
    this
  }

  /**
   * Set the number of sub models to be trained in parallel. Default 20.
   */
  def setNumSubModels(num: Int): this.type = {
    numSubModels = num
    this
  }
  /**
   * Set rho which is the augmented Lagrangian parameter.
   * kappa = regParam / (rho * numSubModels), if the absolute value of element in model
   * is less than kappa, this element will be assigned to zero.
   * So kappa should be less than 0.01 or 0.001.
   */
  def setRho(factor: Double): this.type = {
    rho = factor
    this
  }

  def train(input: String, testSet: String): Model = {
    val instances = DataLoader.loadOneHotInstance(input, partitionNum, sampleRate, -1).rdd
        .map { row =>
          Tuple2(row.getString(0).toDouble, row.getAs[mutable.WrappedArray[Int]](1).toArray)
        }

    val featLength = instances.map { case (label, feature) => feature.max }.max() + 1
    println(s"feat length: $featLength")

    val testRDD = if (testSet != null && testSet != "") {
      DataLoader.loadOneHotInstance(testSet, partitionNum, sampleRate, featLength - 1).rdd
        .map { row =>
          Tuple2(row.getString(0).toDouble, row.getAs[mutable.WrappedArray[Int]](1).toArray)
        }
    } else {
      null
    }

    val lr = new ADMM(new LogisticGradient, new L1Updater)
      .setRegParam(regParam)
      .setNumIterations(maxIter)
      .setNumSubModels(numSubModels)
      .setRho(rho)
      .setTestSet(testRDD)

    val initModel = Vectors.zeros(featLength).toDense
    val (weight, lossHistory) = lr.optimize(instances, initModel)

    println(s"lr loss history: ${lossHistory.mkString(" ")}")
    val lrModel = new SparseModel(weight)

    lrModel
  }

  def predict(input: String, output: String, model: Model): Unit = {
    val featSize = model.asInstanceOf[SparseModel].weight.size - 1
    val instances = DataLoader.loadOneHotInstance(input, partitionNum, sampleRate, featSize - 1)
    println(s"predict instance count: ${instances.count()}")

    val predictDF = model.predict(instances)
    DataSaver.save(predictDF, output)
  }

  override def loadModel(modelPath: String): Model = {
    val sc = SparkContext.getOrCreate()
    val weight = sc.textFile(modelPath + "/weight")
      .map { line =>
        val items = line.split(":")
        (items(0).toInt, items(1).toDouble)
      }.sortByKey()
      .map(_._2)
      .collect()

    println(s"load model successfully, model length: ${weight.length}")
    new SparseModel(new DenseVector(weight))
  }
}

class SparseModel(val weight: DenseVector) extends Model {

  override def save(path: String): Unit = {
    val sc = SparkContext.getOrCreate()

    val modelPath = path + "/weight"
    val conf = sc.hadoopConfiguration
    val fs = new Path(modelPath).getFileSystem(conf)
    if (fs.exists(new Path(modelPath))) {
      fs.delete(new Path(modelPath), true)
    }

    sc.parallelize(weight.toArray, 10).zipWithIndex()
      .map { case (w, index) => index + ":" + w}
      .saveAsTextFile(modelPath)
  }

  override def predict(input: DataFrame): DataFrame = {
    val sc = SparkContext.getOrCreate()
    val modelBC = sc.broadcast(weight)
    val modelLength = weight.size
    val predictUDF = udf { (feature: mutable.WrappedArray[Int]) =>
      val weightSum = feature.filter(index => index < modelLength)
        .map { index => modelBC.value.values(index) }.sum
      1.0 / (1 + math.exp(-1 * weightSum))
    }
    input.withColumn(DFStruct.PROB, predictUDF(col(DFStruct.FEATURE))).drop(DFStruct.FEATURE)
  }

  override def evaluate(testSet: DataFrame, evaluator: Evaluator): Double = 0.5

  override def summary(): String = null
}


object SparseLogisticRegression {

  def main(args: Array[String]): Unit = {
    println(s"Start Sparse Logistic Regression Processing...")

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse(ParamKeys.MODE, "yarn-cluster")
    val input = params.getOrElse(ParamKeys.INPUT, null)
    val sampleRate = params.getOrElse(ParamKeys.SAMPLE_RATE, "1.0").toDouble
    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "100").toInt

    // algorithm parameter
    val numSubModel = params.getOrElse("numSubModel", "10").toInt
    val regParam = params.getOrElse(ParamKeys.REG_PARAM, "0.0").toDouble
    val rho = params.getOrElse("rho", "0.1").toDouble
    val maxIter = params.getOrElse(ParamKeys.MAX_ITER, "15").toInt

    // system parameter
    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, "train")
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)
    val testSet = params.getOrElse(ParamKeys.TEST_SET, "")
    val output = params.getOrElse(ParamKeys.OUTPUT, null)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    if (mode.startsWith("local")) spark.sparkContext.setLogLevel("INFO")
    PSClient.setup(spark.sparkContext)

    val lr = new SparseLogisticRegression()
        .setPartitionNum(partitionNum)
        .setSampleRate(sampleRate)
        .setNumSubModels(numSubModel)
        .setRegParam(regParam)
        .setRho(rho)
        .setMaxIter(maxIter)

    lr.process(actionType, input, modelPath, testSet, output)

    spark.stop()
  }

}
