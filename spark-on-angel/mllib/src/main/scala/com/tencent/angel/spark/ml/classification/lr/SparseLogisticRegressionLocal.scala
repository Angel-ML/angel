package com.tencent.angel.spark.ml.classification.lr

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.util._
import scala.collection.mutable
import com.tencent.angel.spark.ml.util.ADMM

/**
 * Created by isakjiang on 2020/12/30.
 */

class SparseLogisticRegression extends Learner {
  private var partitionNum: Int = _
  private var sampleRate: Double = _
  private var regParam = 0.0
  private var maxIter = 15
  private var numSubModels = 100
  private var rho = 0.01
  private var sep = " "
  private var summaryPath: String = _

  def setPartitionNum(num: Int): this.type = {
    partitionNum = num
    this
  }

  def setSep(sep: String): this.type = {
    this.sep = sep
    this
  }

  def setSampleRate(rate: Double): this.type = {
    sampleRate = rate
    this
  }

  def setRegParam(reg: Double): this.type = {
    regParam = reg
    this
  }

  def setMaxIter(num: Int): this.type = {
    maxIter = num
    this
  }

  def setNumSubModels(num: Int): this.type = {
    numSubModels = num
    this
  }

  def setRho(factor: Double): this.type = {
    rho = factor
    this
  }

  def setSummaryPath(path: String): this.type = {
    summaryPath = path
    this
  }

  override def train(input: String, testSet: String): Model = {
    val sc = SparkContext.getOrCreate()
    val instances = DataLoader.loadOneHotInstance(sc, input, partitionNum, sampleRate, sep = sep).rdd
      .map { case Row(label: String, feature: mutable.WrappedArray[Int]) => Tuple2(label.toDouble, feature.toArray) }

    val featLength = instances.map { case (label, feature) => feature.max }.max() + 1
    LogUtils.logTime(s"feat length: $featLength")

    val testRDD = if (testSet != null && testSet != "") {
      DataLoader.loadOneHotInstance(sc, testSet, partitionNum, sampleRate, featLength - 1, sep = sep).rdd
        .map { case Row(label: String, feature: mutable.WrappedArray[Int]) => Tuple2(label.toDouble, feature.toArray) }
    } else {
      sc.emptyRDD[(Double, Array[Int])]
    }

    val lr = new ADMM(new LogisticGradient, new L1Updater)
      .setRegParam(regParam)
      .setNumIterations(maxIter)
      .setNumSubModels(numSubModels)
      .setRho(rho)
      .setMetricSummary(summaryPath)
      .setTestSet(testRDD)

    val initModel = Vectors.zeros(featLength).toDense
    val (weight, lossHistory) = lr.optimize(instances, initModel)

    LogUtils.logTime(s"lr loss history: ${lossHistory.mkString(" ")}")
    val lrModel = new SparseModel(weight)

    lrModel
  }

  override def predict(input: String, output: String, model: Model) = {
    val sc = SparkContext.getOrCreate()
    val featSize = model.asInstanceOf[SparseModel].weight.size - 1
    val instances = DataLoader.loadOneHotInstance(sc, input, partitionNum, sampleRate, featSize - 1, sep = sep)
    LogUtils.logTime(s"predict instance count: ${instances.count()}")

    val predictDF = model.predict(instances)
    DataSaver.save(predictDF, output, sep)
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

    LogUtils.logTime(s"load model successfully, model length: ${weight.length}")
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
      .map { case (w, index) => index + ":" + w }
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

  override def evaluate(testSet: DataFrame, evaluator: Evaluator): Double = ???

  override def export(): PMMLExportable = ???

  override def summary(): String = ???

  override def toPMML(): String = ???
}


object SparseLogisticRegression {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime(s"Start Sparse Logistic Regression Processing...")

    val mode = "local"
    val input = "data/mlTestData/SLR_data_test.csv"
    val partitionNum = 1
    val sampleRate = 1
    val sep = ","


    // algorithm parameter
    val numSubModel = 5
    val regParam = 0
    val rho = 0.1
    val maxIter = 5

    // system parameter
    val actionType = "predict"//"predict"//"train"
    val modelPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outputModelSLR"
    val testSet = ""
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outputSLR"

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lr = new SparseLogisticRegression()
      .setPartitionNum(partitionNum)
      .setSampleRate(sampleRate)
      .setNumSubModels(numSubModel)
      .setRegParam(regParam)
      .setRho(rho)
      .setMaxIter(maxIter)
      .setSummaryPath(modelPath)
      .setSep(sep)

    lr.process(actionType, input, modelPath, testSet, output)

    sc.stop()
  }

}
