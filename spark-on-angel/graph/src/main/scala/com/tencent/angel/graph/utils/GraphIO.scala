/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.graph.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object GraphIO {

  private val DELIMITER = "delimiter"
  private val HEADER = "header"

  private val int2Long = udf[Long, Int](_.toLong)
  private val string2Long = udf[Long, String](_.toLong)
  private val int2Float = udf[Float, Int](_.toFloat)
  private val long2Float = udf[Float, Long](_.toFloat)
  private val double2Float = udf[Float, Double](_.toFloat)
  private val string2Float = udf[Float, String](_.toFloat)
  private val long2Int = udf[Int, Long](_.toInt)
  private val string2Int = udf[Int, String](_.toInt)

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
      case t => throw new Exception(s"$t can't convert to Float")
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

  def convert2Int(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: IntegerType => df
      case _: LongType =>
        df.withColumn(tmpName, long2Int(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: StringType =>
        df.withColumn(tmpName, string2Int(df(structField.name)))
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
        StructField("dst", LongType, nullable = false),
        StructField("weight", FloatType, nullable = false)
      ))
    } else {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false)
      ))
    }
    ss.read
      .option("sep", sep)
      .option("header", "false")
      .schema(schema)
      .csv(input)
  }

  def loadNode(input: String, index: Int = 0, sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val schema = StructType(Seq(StructField("node", LongType, nullable = false)))
    ss.read
      .option("sep", sep)
      .option("header", "false")
      .schema(schema)
      .csv(input)
  }

  def loadInt(input: String, isWeighted: Boolean,
              srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
              sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val schema = if (isWeighted) {
      StructType(Seq(
        StructField("src", IntegerType, nullable = false),
        StructField("dst", IntegerType, nullable = false),
        StructField("weight", FloatType, nullable = false)
      ))
    } else {
      StructType(Seq(
        StructField("src", IntegerType, nullable = false),
        StructField("dst", IntegerType, nullable = false)
      ))
    }
    ss.read
      .option("sep", sep)
      .option("header", "false")
      .schema(schema)
      .csv(input)
  }

  def loadNodeAttrs(input: String, nodeIndex: Int = 0, attrIndexes: Array[Int], sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val se1 = Array(StructField("node", LongType, nullable = false))
    val se = attrIndexes.map(x => StructField("attr_" + x, FloatType, nullable = false))
    val schema = StructType((se1 ++ se).toSeq)
    ss.read
      .option("sep", sep)
      .option("header", "false")
      .schema(schema)
      .csv(input)
  }

  def save(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    df.write
      .mode(SaveMode.Overwrite)
      .option(HEADER, "false")
      .option(DELIMITER, seq)
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