package com.tencent.angel.spark.ml.featurePreprocess.StringIndexer

import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{AngelStringIndexer, AngelStringIndexerModel}

object StringIndexerLocal {
  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/mlTestData/stringIndexerTestData"
    val output = "data/output/stringIndexerOutput"
    val partitionNum = 1
    val sampleRate = 1
    val sep = ","
    val featureCols = "1-2"
    val handleInvalid = "skip" // keep,skip,error
    val stringOrderType = "frequencyDesc" //frequencyDesc,frequencyAsc,alphabetDesc,alphabetAsc
    val actionType = "predict" //train,predict
    val modelPath = "data/output/modelPath"


    val sc = start(mode)
    actionType match {
      case "train" =>
        val model = train(sc, input, output, featureCols, handleInvalid, stringOrderType, partitionNum, sampleRate, sep)
        model.write.overwrite().save(modelPath)
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
    inputDF.show(20)

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
    resultDF.show(20)
    DataSaver.save(resultDF, output, sep)

    model
  }

  def predict(sc: SparkContext, input: String, output: String, model: AngelStringIndexerModel, featureCols: String, partitionNum: Int, sampleRate: Double, sep: String): Unit = {
    val inputDF = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)
    inputDF.show(20)

    val featureSections = ArgsUtil.parseFeatureCols(featureCols)
    val inputCols = featureSections.flatMap { case (begin, end) => inputDF.columns.slice(begin, end + 1) }

    model.setInputCols(inputCols)
    val resultDF = model.transform(inputDF)
    resultDF.show(20)
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

