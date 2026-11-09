package com.tencent.angel.spark.ml.time_series.transformation

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by isakjiang on 2021/02/26.
 */
object DifferentiatedLocal {
  def main(args: Array[String]): Unit = {
    val input = "data/mlTestData/example_air_passengers_unix_data"
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outPACF"

    val timeCol = 0
    val timeSeriesCol = 1

    val partitionNum = 1
    val sep = ","
    val sampleRate = 1


    val d = 2
    val inverse = false

    val mode = "local"
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

    //println("---------------")
    //data.show(20)
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
