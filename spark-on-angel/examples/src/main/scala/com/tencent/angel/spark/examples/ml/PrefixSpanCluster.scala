package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, LogUtils, ParamKeys}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.fpm.PrefixSpan
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import scala.collection.mutable

/**
 * Created by isakjiang on 2021/09/01.
 */
object PrefixSpanCluster {
  def main(args: Array[String]) {
    LogUtils.logTime("PrefixSpan start...")
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
    val maxLength = params.getOrElse("maxLength", "10").toInt
    val dataIndex = params.getOrElse("dataIndiex", "0").toInt

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()

    run(spark, input, output, minSupport, maxLength, partitionNum, sep, dataIndex)
  }

  def run(spark: SparkSession,
          input: String,
          output: String,
          minSupport: Double,
          maxLength: Int,
          partitionNum: Int,
          sep: String,
          dataIndex: Int): Unit = {

    val dataDF = DataLoader.loadDataItems(input, partitionNum, sep, dataIndex, true)(SparkContext.getOrCreate())

    var result = new PrefixSpan()
      .setMinSupport(minSupport)
      .setSequenceCol("items")
      .setMaxPatternLength(maxLength)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(dataDF)

    def arrayToString(s: mutable.WrappedArray[mutable.WrappedArray[String]]): String = {
      s.map(x => x.mkString("|")).mkString("#")
    }

    val arrayToStringUDF: UserDefinedFunction = udf[String, mutable.WrappedArray[mutable.WrappedArray[String]]](arrayToString)

    result = result.select(arrayToStringUDF(col("sequence")).as("itemset"), col("freq")).repartition(partitionNum)
    DataSaver.save(result, output, sep)
    spark.close()
  }

}
