package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader, DataSaver, ParamKeys}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{AngelStringIndexer, AngelStringIndexerModel}

object StringIndexerCluster {
  def main(args: Array[String]): Unit = {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    val handleInvalid = params.getOrElse("handleInvalid", "keep") //keep,skip,error
    val stringOrderType = params.getOrElse("stringOrderType", "frequencyDesc") //frequencyDesc,frequencyAsc,alphabetDesc,alphabetAsc
    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, null)
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)


    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start(mode)
    actionType match {
      case "train" =>
        val model = train(sc, input, output, featureCols, handleInvalid, stringOrderType, partitionNum, sampleRate, sep)
        if (modelPath != "") {
          model.write.overwrite().save(modelPath)
        }
      case "predict" =>
        val model = AngelStringIndexerModel.load(modelPath)
        predict(sc, input, output, model, featureCols, partitionNum, sampleRate, sep)
      case _ =>
        println(s"$actionType do not support")
    }
    stop()

  }

  def train(sc: SparkContext, input: String, output: String, featureCols: String, handleInvalid: String, stringOrderType: String, partitionNum: Int, sampleRate: Double, sep: String): AngelStringIndexerModel = {
    val inputDF = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val featureSections = ArgsUtil.parseFeatureCols(featureCols)
    val inputCols = featureSections.flatMap { case (begin, end) => inputDF.columns.slice(begin, end + 1) }
    val outputCols = inputCols.map(i => i + "_indexerCol")

    val stringIndexer = new AngelStringIndexer()
      .setHandleInvalid(handleInvalid)
      .setStringOrderType(stringOrderType)
      .setInputCols(inputCols)
      .setOutputCols(outputCols)

    val model = stringIndexer.fit(inputDF)
    val resultDF = model.transform(inputDF)
    DataSaver.save(resultDF, output, sep)
    model
  }

  def predict(sc: SparkContext, input: String, output: String, model: AngelStringIndexerModel, featureCols: String, partitionNum: Int, sampleRate: Double, sep: String): Unit = {
    val inputDF = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)
    val featureSections = ArgsUtil.parseFeatureCols(featureCols)
    val inputCols = featureSections.flatMap { case (begin, end) => inputDF.columns.slice(begin, end + 1) }

    model.setInputCols(inputCols)
    val resultDF = model.transform(inputDF)
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

