package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util._
import org.apache.spark.ml.feature.VectorDisassembler
import org.apache.spark.ml.feature.select.TreeBasedSelector
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by isakjiang on 2022/10/29.
 */
object TreeBasedSelectorCluster {

  def main(args: Array[String]) {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val featureCols = params.getOrElse("featureCols", null)
    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, MAX_INT_STR).toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val numTopFeatures = params.getOrElse("numTopFeatures", "50").toInt
    val selectorType = params.getOrElse("selectorType", "numTopFeatures")
    val taskType = params.getOrElse("taskType", "classifier")

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
    val data = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, featureColExpr, labelCol, false, sep)

    val treeBasedSelector = new TreeBasedSelector()
      .setNumTopFeatures(numTopFeatures)
      .setFeaturesCol(DFStruct.FEATURE)
      .setLabelCol(DFStruct.LABEL)
      .setOutputCol("TreeBasedResult")
      .setSelectorType(selectorType)
      .setTaskType(taskType)

    var resultDF = treeBasedSelector.fit(data)
      .transform(data).drop(DFStruct.FEATURE)

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
