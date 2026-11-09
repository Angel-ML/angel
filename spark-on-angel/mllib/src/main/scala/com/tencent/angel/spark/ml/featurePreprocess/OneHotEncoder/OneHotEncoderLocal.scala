package com.tencent.angel.spark.ml.featurePreprocess.OneHotEncoder

import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader, DataSaver}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * Created by isakjiang on 2022/11/02.
 */
object OneHotEncoderLocal {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    val mode = "local"
    val input = "data/mlTestData/onehot_test.txt"
    val featureCols = "3,4"
    val output = "data/output/outOneHot"

    val sep = ","
    val partitionNum = 1
    val sampleRate = 1

    val sc = start(mode)
    run(sc, input, output, featureCols, partitionNum, sampleRate, sep)
    stop()

  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          featureCols: String,
          partitionNum: Int,
          sampleRate: Double,
          sep: String): Unit = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, null, sep)
    data.show(5)

    val featureSections = ArgsUtil.parseFeatureCols(featureCols)
    val inputCols = featureSections.flatMap { case (begin, end) =>
      data.columns.slice(begin, end + 1)
    }
    println(inputCols.mkString(","))
    inputCols.foreach { colName =>
      data = data.withColumn(colName, col(colName).cast(DoubleType))
    }
    data.show(3)

    val outputCols = inputCols.map(i => i + "_onehot")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(inputCols)
      .setDropLast(false)
      .setOutputCols(outputCols)

    var resultDF = oneHotEncoder.fit(data).transform(data).drop(inputCols: _*)
    println(inputCols.mkString(","))
    println("after OneHotEncoderEstimator")
    resultDF.show(3)


    outputCols.foreach { colName =>
      val size = resultDF.select(colName).head().get(0).asInstanceOf[SparseVector].size
      val outputColNames = (0 until size).map(i => colName + "_" + i.toString).toArray
      resultDF = new VectorDisassembler()
        .setInputCol(colName)
        .setOutputCols(outputColNames)
        .transform(resultDF).drop(colName)
    }
    resultDF.cache()

    println("the examples of final result  is ")
    resultDF.show(3)
    DataSaver.save(resultDF, output, sep)

  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}
