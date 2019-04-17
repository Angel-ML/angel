package com.tencent.angel.spark.ml.graph.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object GraphIO {

  private val DELIMITER = "delimiter"
  private val HEADER = "header"

  def load(input: String, isWeighted: Boolean): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val schema = if (isWeighted) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false)
      ))
    } else {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("wgt", LongType, nullable = false)
      ))
    }
    ss.read
      .option("sep", " ")
      .option("header", "false")
      .schema(schema)
      .csv(input)
  }

  def save(df: DataFrame, output: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option(HEADER, "false")
      .option(DELIMITER, "\t")
      .csv(output)
  }

  def defaultCheckpointDir: Option[String] = {
    val sparkContext = SparkContext.getOrCreate()
    sparkContext.getConf.getOption("spark.yarn.stagingDir")
      .map { base =>
        new Path(base, s".sparkStaging/${sparkContext.getConf.getAppId}").toString
      }
  }
}
