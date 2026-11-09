package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util._
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.{PCAModel, PCA => MLPCA}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * PCA 主成分分析
 *
 * 参数说明:
 * ******** system params ***
 * mode           | 运行模式
 * ******** input params ***
 * input          | 输入数据路径
 * inputType      | 输入存储类型
 * partitionNum   | 数据分区数
 * sampleRate     | 抽样率
 * ******** output params ***
 * output         | 输出数据路径
 * outputType     | 输出存储类型
 * ******** algorithm params ***
 * k              | 提取主成份维度
 */

object PCACluster {

  def main(args: Array[String]) {

    LogUtils.logTime("PCA feature extraction start...")
    // for PCA algorithm

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val featureCols = params.getOrElse("featureCols", null)
    val output = params.getOrElse("output", null)
    val pcOutput = params.getOrElse("pcOutput", null)
    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, "train")
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val k = params.getOrElse("K", "3").toInt

    val sc = start(mode)
    run(sc, input, output, partitionNum, sampleRate, featureCols, k, pcOutput, sep, actionType, modelPath)
    stop()
  }

  /** 主成分分析
   *
   * @param sc
   * @param input
   * @param output
   * @param partitionNum
   * @param sampleRate
   * @param k
   */

  def run(sc: SparkContext,
          input: String,
          output: String,
          partitionNum: Int,
          sampleRate: Double,
          featureCols: String,
          k: Int,
          pcOutput: String,
          sep: String,
          actionType: String,
          modelPath: String) {

    val inputDF = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false, sep)

    val resFeature = "pcaFeatures"

    val pcaModel = actionType match {
      case "train" =>
        new MLPCA()
          .setInputCol(DFStruct.FEATURE)
          .setOutputCol(resFeature)
          .setK(k)
          .fit(inputDF)
      case "predict" =>
        PCAModel.load(modelPath)
    }

    // print PCA model
    if (pcOutput != null && actionType == "train") {
      saveModel(pcaModel, pcOutput)
    }

    if (modelPath != null && actionType == "train") {
      pcaModel.save(modelPath)
    }

    val pcaDF = pcaModel.transform(inputDF).drop(DFStruct.FEATURE)
    DataSaver.save(pcaDF, output, sep)
  }

  def saveModel(pcaModel: PCAModel, savePath: String) {

    val pcMatrix = pcaModel.pc

    val numCols = pcMatrix.numCols
    val numRows = pcMatrix.numRows

    val rowString = (0 until numRows).toArray.map { row =>
      (0 until numCols).map { col =>
        pcMatrix.apply(row, col)
      }.mkString(" ")
    }

    val sc = SparkContext.getOrCreate()

    val path = new Path(savePath)
    val conf = sc.hadoopConfiguration
    val fs = path.getFileSystem(conf)
    if (fs.exists(path)) fs.delete(path, true)

    sc.parallelize(rowString, 1).saveAsTextFile(savePath)
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
