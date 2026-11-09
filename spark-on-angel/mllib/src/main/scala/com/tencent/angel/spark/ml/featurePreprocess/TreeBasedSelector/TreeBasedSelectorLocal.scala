package com.tencent.angel.spark.ml.featurePreprocess.TreeBasedSelector

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver}
import org.apache.spark.ml.feature.VectorDisassembler
import org.apache.spark.ml.feature.select.TreeBasedSelector
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2022/10/29.
 */
object TreeBasedSelectorLocal {

  def main(args: Array[String]) {

    val mode = "local"
    val input = "data/mlTestData/iris_two_class.csv"
    val featureCols = "0-3"
    val sep = ","
    val output = "data/output/outVI"
    val partitionNum = 1
    val sampleRate = 1

    // special for the features which are expressed as denseVector

    val labelCol = 4
    val numTopFeatures = 2

    val selectorType = "numTopFeatures"
    val taskType = "classifier"


    val sc = start(mode)
    run(sc, input, output, featureCols, labelCol, numTopFeatures, selectorType, taskType, partitionNum, sampleRate, sep)
    stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          featureColExpr: String,
          labelCol: Int,
          numTopFeatures: Int,
          selectorType: String,
          taskType: String,
          partitionNum: Int,
          sampleRate: Double,
          sep: String
         ): Unit = {
    // "features" as denseVector for features and "label" as labelCol
    val data = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate,  featureColExpr, labelCol,false,sep)

    data.show(5)
    val treeBasedSelector = new TreeBasedSelector()
      .setNumTopFeatures(numTopFeatures)
      .setFeaturesCol(DFStruct.FEATURE)
      .setLabelCol(DFStruct.LABEL)
      .setOutputCol("TreeBasedResult")
      .setSelectorType(selectorType)
      .setTaskType(taskType)

    var resultDF = treeBasedSelector.fit(data)
      .transform(data).drop(DFStruct.FEATURE)

    resultDF.show(5)

    resultDF = new VectorDisassembler()
      .setInputCol("TreeBasedResult").transform(resultDF).drop("TreeBasedResult")

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
