package com.tencent.angel.spark.ml.time_series.test

import com.tencent.angel.spark.ml.util._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2021/02/26.
 */
object ADFTestLocal {
  def main(args: Array[String]): Unit = {
    val input = "data/mlTestData/example_air_passengers_unix_data"
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outACF"

    val timeCol = 0
    val timeSeriesCol = 1

    val maxLag = 5
    val regression = "ct"
    val mode = "local"

    val partitionNum = 1
    val sep = ","
    val sampleRate = 1

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")


    run(sc, input, output, timeCol, timeSeriesCol, maxLag, partitionNum, sampleRate, sep, regression)
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          timeCol: Int,
          timeSeriesCol: Int,
          maxLag: Int,
          partitionNum: Int,
          sampleRate: Double,
          sep: String,
          regression: String): Unit = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, "", sep)

    val timeColName = data.schema.fields(timeCol).name
    val timeSeriesColName = data.schema.fields(timeSeriesCol).name


    //val res = data.select(unix_timestamp(col("date"), "yyyy-MM-dd").as("timestamp_3"))//, unix_timestamp(timeColName, "yyyy/MM/dd HH:mm:ss").as("timestamp")
    //val res = data.withColumn(timeColName, unix_timestamp(col(timeColName), "yyyy-MM-dd"))
    //res.show(10)
    //DataSaver.save(res, output, sep)

    data = data.withColumn(timeSeriesColName, data(timeSeriesColName).cast(DoubleType))

    data.show(10)
    println(data.schema)

    val adfTest = new ADFTest()
      .setMaxLag(maxLag)
      .setRegression(regression)
      .setTimeCol(timeColName)
      .setTimeSeriesCol(timeSeriesColName)

    val resultDF = adfTest.transform(data)

    DataSaver.save(resultDF, output, sep)


  }


}
