package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}

/**
 * Created by isakjiang on 2020-11-16
 */
object SpliterExample {
  def main(args: Array[String]): Unit = {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val output1 = params.getOrElse("output1", null)
    val output2 = params.getOrElse("output2", null)

    val fraction = params.getOrElse("fraction", "1.0").toDouble
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

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
