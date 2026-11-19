package com.tencent.angel.spark.ml.featurePreprocess.ChiSqSelector

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver}
import org.apache.spark.ml.feature.{ChiSqSelector => mlChiSqSelector}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2020-11-16
 * should not support the libsvm input
 */
object ChiSqSelectorLocal {

  val NUMTOPFEATURES = "numTopFeatures"

  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetInput/wine.data"
    val partitionNum = 1
    val sampleRate = 1
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outputChiSq"
    val sep = ","

    //special for the features which are expressed as denseVector
    val colStr = "1-10"
    val labelCol = 0
    val numTopFeatures = 5

    val ss = start(mode)

    run(ss,
      input,
      partitionNum,
      sampleRate,
      colStr,
      labelCol,
      numTopFeatures,
      output,
      sep)

    stop()
  }

  def run(sc: SparkContext,
          input: String,
          partitionNum: Int,
          sampleRate: Double,
          colStr: String,
          labelCol: Int,
          numTopFeatures: Int,
          output: String,
          sep: String) = {
    //outputColName
    val outColName = "ChiSqResult"

    //"features" as denseVector for features and "label" as labelCol
    val inputDF = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, colStr, labelCol, false, sep)

    val chiSqSelector = new mlChiSqSelector()
      .setNumTopFeatures(numTopFeatures)
      .setFeaturesCol(DFStruct.FEATURE)
      .setLabelCol(DFStruct.LABEL)
      .setOutputCol(outColName)

    val chiSqResult = chiSqSelector.fit(inputDF).transform(inputDF).drop(DFStruct.FEATURE)

    DataSaver.save(chiSqResult, output, sep)

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
