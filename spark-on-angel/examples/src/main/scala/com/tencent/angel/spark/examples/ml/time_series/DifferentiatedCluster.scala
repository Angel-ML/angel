package com.tencent.angel.spark.examples.ml.time_series

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.time_series.transformation.Differentiated
import com.tencent.angel.spark.ml.core.ArgsUtil

/**
 * Created by isakjiang on 2021/02/26.
 */
object DifferentiatedCluster {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val output = params.getOrElse("output", null)
    val timeCol = params.getOrElse("timeCol", "0").toInt
    val timeSeriesCol = params.getOrElse("timeSeriesCol", "1").toInt
    val d = params.getOrElse("d", "2").toInt
    val inverse = params.getOrElse("inverse", "false").toBoolean

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    run(sc, input, output, timeCol, timeSeriesCol, d, inverse, partitionNum, sampleRate, sep)
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          timeCol: Int,
          timeSeriesCol: Int,
          d: Int,
          inverse: Boolean,
          partitionNum: Int,
          sampleRate: Double,
          sep: String): Unit = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name

    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    val diff = new Differentiated()
      .setD(d)
      .setInverse(inverse)
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)

    val resultDF = diff.transform(data)

    val spark = data.sparkSession
    import spark.implicits._

    if (!inverse) {
      val dataAfter = resultDF.select(timeColName, timeSeriesColName).rdd.map {
        case Row(time: Double, value: Double) => (time, value)
        case Row(time: Int, value: Double) => (time.toDouble, value)
        case Row(time: Float, value: Double) => (time.toDouble, value)
        case Row(time: String, value: Double) => (time.toDouble, value)
      }.sortByKey().collect().drop(d)

      val dataVector = dataAfter.map(x => x._2)
      val indexVector = (0 until dataVector.length - d).toVector
      val result = dataVector.zip(indexVector)

      val resDF = spark.createDataset(result).toDF(timeColName, timeSeriesColName)
      DataSaver.save(resDF, output, sep)
    }
    else {
      DataSaver.save(resultDF, output, sep)
    }

    spark.close()
  }
}
