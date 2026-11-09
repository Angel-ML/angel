package com.tencent.angel.spark.examples.ml.time_series

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.time_series.stat.PACF
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2021/02/26.
 */
object PACFCluster {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val output = params.getOrElse("output", null)
    val timeCol = params.getOrElse("timeCol", "0").toInt
    val timeSeriesCol = params.getOrElse("timeSeriesCol", "1").toInt
    val maxLag = params.getOrElse("maxLag", "40").toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    run(sc, input, output, timeCol, timeSeriesCol, maxLag, partitionNum, sampleRate, sep)
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          timeCol: Int,
          timeSeriesCol: Int,
          maxLag: Int,
          partitionNum: Int,
          sampleRate: Double,
          sep: String): Unit = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name
    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    val pacf = new PACF()
      .setMaxLag(maxLag)
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)

    val resultDF = pacf.transform(data)
    DataSaver.save(resultDF, output, sep)
  }
}
