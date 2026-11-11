package com.tencent.angel.spark.ml.featurePreprocess.Normalizer

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver, LogUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{Normalizer => MLNorm}
/**
 * Normalizer
 *
 * 参数说明:
 *
 * mode         local or yarn
 * input         input file
 * inputType     文件类型 (HDFS/local)
 * beginCol      特征起始列 (从 0 开始计数, 包含当前列)
 * endCol        特征终止列 (从 0 开始计数, 包含当前列)
 * partitionNum   partition数目
 * sampleRate    采样率. 默认:1.0 [optional]
 * ouput         结果输出路径
 * ouputType     结果存储类型 (HDFS/local)
 * p             p-norm
 *
 * Created by isakjiang on 2020-11-16
 */

object NormalizerLocal {
  val NormFeature = "norm_feature"
  val P_NORM_KEY = "p"

  def main(args: Array[String]): Unit = {
    LogUtils.logTime("Start Normalize Processing...")
    val mode = "local"
    val input = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetInput/wine.data"
    val featureCols = "1-3"
    val sampleRate = 1
    val partitionNum = 1
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/output"

    // for algorithm
    val p = 2
    val sep = ","

    val sc = start(mode)
    run(p, input, featureCols, partitionNum, sampleRate, output, sc, sep)
    stop()
  }

  private def run(
                   p: Double,
                   input: String,
                   featureCols: String,
                   partitionNum: Int,
                   sampleRate: Double,
                   output: String,
                   sc: SparkContext,
                   sep: String): Unit = {
    val inputDF = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false,sep)

    val normalizer = new MLNorm()
      .setP(p)
      .setInputCol(DFStruct.FEATURE)
      .setOutputCol(NormFeature)

    val resultDF = normalizer.transform(inputDF)
      .drop(DFStruct.FEATURE)

    DataSaver.save(resultDF, output,sep)
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
