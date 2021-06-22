package com.tencent.angel.spark.ml.featureEngineering.Spliter

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2020-11-16
 */
object SpliterLocal {
  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "data/mlDataTest/wine.data"
    val output1 = "data/output/output1"
    val output2 = "data/output/output2"
    val partitionNum = 1
    val fraction = 0.5
    val sep = ","

    val sc = start(mode)

    val inputTable = DataLoader.loadByLine(sc, input, partitionNum, 1.0)

    require(fraction < 1.0 && fraction > 0.0, "split fraction should between 0.0 and 1.0")
    val dfs = inputTable.randomSplit(Array(fraction, 1.0 - fraction))

    DataSaver.save(dfs(0), output1, sep)
    DataSaver.save(dfs(1), output2, sep)

    stop()
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
