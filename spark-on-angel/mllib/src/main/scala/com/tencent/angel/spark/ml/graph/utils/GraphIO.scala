package com.tencent.angel.spark.ml.graph.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object GraphIO {

  private val DELIMITER = "delimiter"
  private val HEADER = "header"

  private val int2Long = udf[Long, Int](_.toLong)
  private val string2Long = udf[Long, String](_.toLong)
  private val int2Float = udf[Float, Int](_.toFloat)
  private val long2Float = udf[Float, Long](_.toFloat)
  private val double2Float = udf[Float, Double](_.toFloat)
  private val string2Float = udf[Float, String](_.toFloat)

  def convert2Float(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: LongType =>
        df.withColumn(tmpName, long2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: IntegerType =>
        df.withColumn(tmpName, int2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: DoubleType =>
        df.withColumn(tmpName, double2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: StringType =>
        df.withColumn(tmpName, string2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: FloatType => df
      case t => throw new Exception(s"$t can't convert to Long")
    }
  }

  def convert2Long(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: LongType => df
      case _: IntegerType =>
        df.withColumn(tmpName, int2Long(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: StringType =>
        df.withColumn(tmpName, string2Long(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case t => throw new Exception(s"$t can't convert to Long")
    }
  }

  def load(input: String, isWeighted: Boolean,
           srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
           sep: String = " "): DataFrame = {
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
      .option("sep", sep)
      .option("header", "false")
      .schema(schema)
      .csv(input)
  }

  def save(df: DataFrame, output: String): Unit = {
    df.printSchema()
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
