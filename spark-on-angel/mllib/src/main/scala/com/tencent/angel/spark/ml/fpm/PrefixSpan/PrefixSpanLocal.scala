package com.tencent.angel.spark.ml.fpm.PrefixSpan

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, LogUtils}
import org.apache.spark.ml.fpm.PrefixSpan
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Created by isakjiang on 2021/09/01.
 */
object PrefixSpanLocal {
  def main(args: Array[String]) {
    LogUtils.logTime("PrefixSpan start...")
    val mode = "local"
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val input = "data/mlTestData/PrefixSpan_data_test"
    val output = "data/output/output"

    val minSupport = 0.25
    val maxLength = 5
    val partitionNum = 1
    val dataIndiex = 0
    val sep = ","

    val spark = SparkSession.builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()
    run(spark, input, output, minSupport, maxLength, partitionNum, sep, dataIndiex)
  }

  def run(spark: SparkSession,
          input: String,
          output: String,
          minSupport: Double,
          maxLength: Int,
          partitionNum: Int,
          sep: String,
          dataIndiex: Int): Unit = {
    println(sep + "fdfdfdfd")
    val dataDF = DataLoader.loadDataItems(input, partitionNum, sep, dataIndiex, true)(SparkContext.getOrCreate())

    dataDF.show()
    println(dataDF.schema)
    var result = new PrefixSpan()
      .setMinSupport(minSupport)
      .setSequenceCol("items")
      .setMaxPatternLength(maxLength)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(dataDF)

    result.show(50)
    println(result.schema)

    def arrayToString(s: mutable.WrappedArray[mutable.WrappedArray[String]]): String = {
      s.map(x => x.mkString("|")).mkString("#")
    }

    val arrayToStringUDF: UserDefinedFunction = udf[String, mutable.WrappedArray[mutable.WrappedArray[String]]](arrayToString)

    result = result.select(arrayToStringUDF(col("sequence")).as("itemset"), col("freq")).repartition(partitionNum)
    DataSaver.save(result, output, sep)
    spark.close()
  }

}
