package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver, ParamKeys}
import org.apache.spark.ml.feature.{VectorDisassembler, VectorIndexer, VectorIndexerModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object VectorIndexerCluster {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  val indexed = "indexed"

  def main(args: Array[String]) {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val featureCols = params.getOrElse("featureCols", null)
    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, null)
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val maxCategories = params.getOrElse("maxCategories", "10").toInt

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

    val vectorIndexer = new VectorIndexer()
      .setMaxCategories(maxCategories)
      .setInputCol(DFStruct.FEATURE)
      .setOutputCol("indexed")

    val model = vectorIndexer.fit(data)
    var resultDF = model.transform(data).drop(DFStruct.FEATURE)

    resultDF = new VectorDisassembler()
      .setInputCol("indexed")
      .transform(resultDF).drop("indexed")

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
