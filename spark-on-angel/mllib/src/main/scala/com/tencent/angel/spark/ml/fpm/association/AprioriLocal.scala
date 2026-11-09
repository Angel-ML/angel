package com.tencent.angel.spark.ml.fpm.association

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, LogUtils}
import org.apache.spark.ml.association.Apriori
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Created by isakjiang on 2021/09/01.
 */
object AprioriLocal {
  def main(args: Array[String]) {
    LogUtils.logTime("KMeans clustering start...")
    val mode = "local"
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val input = "data/mlTestData/GroceryStoreDataSet.csv"
    val output = "data/output/output"

    val minSupport = 0.001
    val maxSupport = 1
    val maxLength = 10
    val minLength = 1
    val partitionNum = 1
    val dataIndiex = 0
    val sep = ","

    val spark = SparkSession.builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()
    run(spark, input, output, minSupport, maxSupport, minLength, maxLength, partitionNum, sep, dataIndiex)
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
          dataIndiex: Int): Unit = {

    val dataDF = DataLoader.loadDataItems(input, partitionNum, sep, dataIndiex)(SparkContext.getOrCreate())

    dataDF.show()
    var result = new Apriori()
      .setMaxLength(maxLength)
      .setMinLength(minLength)
      .setMaxSupport(maxSupport)
      .setMinSupport(minSupport)
      .setInputCol("items")
      .setOutputCols(Array("itemset", "frequency"))
      .transform(dataDF)

    result.show(50)

    def arrayToString(s: mutable.WrappedArray[String]): String = {
      s.mkString("|")
    }

    val arrayToStringUDF: UserDefinedFunction = udf[String, mutable.WrappedArray[String]](arrayToString)

    result = result.select(arrayToStringUDF(col("itemset")).as("itemset"), col("frequency")).repartition(partitionNum)
    DataSaver.save(result, output, sep)
    spark.close()
  }

}
