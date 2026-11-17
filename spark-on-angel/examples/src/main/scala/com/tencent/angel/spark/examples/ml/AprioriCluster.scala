package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, LogUtils, ParamKeys}
import org.apache.spark.ml.association.Apriori
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}

import scala.collection.mutable

/**
 * Created by isakjiang on 2021/09/01.
 */
object AprioriCluster {
  def main(args: Array[String]) {

    LogUtils.logTime("Start Apriori...")

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse(ParamKeys.OUTPUT, null)
    val partitionNum = params.getOrElse("partitionNum", "4").toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val minSupport = params.getOrElse("minSupport", "0.001").toDouble
    val maxSupport = params.getOrElse("maxSupport", "1").toDouble
    val maxLength = params.getOrElse("maxLength", "10").toInt
    val minLength = params.getOrElse("minLength", "1").toInt
    val dataIndex = params.getOrElse("dataIndiex", "0").toInt


    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()
    run(spark, input, output, minSupport, maxSupport, minLength, maxLength, partitionNum, sep, dataIndex)
  }

  def run(spark: SparkSession,
          input: String,
          output: String,
          minSupport: Double,
          maxSupport: Double,
          minLength: Int,
          maxLength: Int,
          partitionNum: Int,
          sep: String,
          dataIndex: Int): Unit = {

    val dataDF = DataLoader.loadDataItems(input, partitionNum, sep, dataIndex)(SparkContext.getOrCreate())

    var result = new Apriori()
      .setMaxLength(maxLength)
      .setMinLength(minLength)
      .setMaxSupport(maxSupport)
      .setMinSupport(minSupport)
      .setInputCol("items")
      .setOutputCols(Array("itemset", "frequency"))
      .transform(dataDF)


    def arrayToString(s: mutable.WrappedArray[String]): String = {
      s.mkString("|")
    }

    val arrayToStringUDF: UserDefinedFunction = udf[String, mutable.WrappedArray[String]](arrayToString)

    result = result.select(arrayToStringUDF(col("itemset")).as("itemset"), col("frequency")).repartition(partitionNum)
    DataSaver.save(result, output, sep)
    spark.close()
  }

}
