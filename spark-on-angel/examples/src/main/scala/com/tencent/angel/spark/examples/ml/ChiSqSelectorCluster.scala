package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import org.apache.spark.ml.feature.{ChiSqSelector => mlChiSqSelector}
/**
 * Created by isakjiang on 2020-11-16
 * should not support the libsvm input
 */
object ChiSqSelectorCluster {

  val NUMTOPFEATURES = "numTopFeatures"

  def main(args: Array[String]): Unit = {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    //special for the features which are expressed as denseVector
    val colStr = params.getOrElse("colStr", null)
    val labelCol = params.getOrElse("labelCol", "0").toInt
    val numTopFeatures = params.getOrElse("numTopFeatures", "50").toInt

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
