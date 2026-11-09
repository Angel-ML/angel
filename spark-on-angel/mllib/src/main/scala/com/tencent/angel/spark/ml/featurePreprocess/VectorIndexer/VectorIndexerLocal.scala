package com.tencent.angel.spark.ml.featurePreprocess.VectorIndexer

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver}
import org.apache.spark.ml.feature.{VectorDisassembler, VectorIndexer, VectorIndexerModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object VectorIndexerLocal {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  val indexed = "indexed"

  def main(args: Array[String]) {
    val mode = "local"

    val input = "data/mlTestData/Dummy_data_test.txt"
    val featureCols = "0-2"
    val sep = " "
    val actionType = "train" //"predict"//"train"
    val modelPath = "data/output/modeLlout_VI"
    val output = "data/output/outVI"
    val partitionNum = 1
    val sampleRate = 1

    val maxCategories = 10

    val sc = start(mode)

    actionType match {
      case "train" =>
        val model = train(sc, input, output, featureCols, maxCategories, partitionNum, sampleRate, sep)
        model.write.overwrite().save(modelPath)

      case "predict" =>
        val model = VectorIndexerModel.load(modelPath)
        predict(sc, input, output, featureCols, model, partitionNum, sampleRate, sep)
      case _ =>
        LOG.error(s"$actionType do not support")
    }

    stop()
  }

  def train(sc: SparkContext,
            input: String,
            output: String,
            featureCols: String,
            maxCategories: Int,
            partitionNum: Int,
            sampleRate: Double,
            sep: String): VectorIndexerModel = {
    val data = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false, sep)

    data.show(3)
    val vectorIndexer = new VectorIndexer()
      .setMaxCategories(maxCategories)
      .setInputCol(DFStruct.FEATURE)
      .setOutputCol("indexed")

    val model = vectorIndexer.fit(data)
    var resultDF = model.transform(data).drop(DFStruct.FEATURE)
    resultDF.show(3)

    resultDF = new VectorDisassembler()
      .setInputCol("indexed")
      .transform(resultDF).drop("indexed")

    resultDF.show(3)
    DataSaver.save(resultDF, output, sep)

    model
  }

  def predict(sc: SparkContext,
              input: String,
              output: String,
              featureCols: String,
              model: VectorIndexerModel,
              partitionNum: Int,
              sampleRate: Double,
              sep: String): Unit = {
    val data = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false, sep)
    var resultDF = model.transform(data).drop(DFStruct.FEATURE)

    resultDF = new VectorDisassembler()
      .setInputCol("indexed")
      .transform(resultDF).drop("indexed")

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
