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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

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
  private val int2String = udf[String, Int](_.toString)
  private val long2String = udf[String, Long](_.toString)

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

  def convert2String(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: StringType => df
      case _: LongType =>
        df.withColumn(tmpName, long2String(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: IntegerType =>
        df.withColumn(tmpName, int2String(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case t => throw new Exception(s"$t can't convert to String")
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

  def loadWithType(input: String, isEdgeType: Boolean, isSrcNodeType: Boolean, isDstNodeType: Boolean,
                   srcIndex: Int = 0, dstIndex: Int = 1, edgeTypeIndex: Int = -1, srcTypeIndex: Int = -1,
                   dstTypeIndex: Int = -1, sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val schema = if (isEdgeType && isSrcNodeType && isDstNodeType) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("edgeType", FloatType, nullable = false),
        StructField("srcType", FloatType, nullable = false),
        StructField("dstType", FloatType, nullable = false)
      ))
    } else if (isEdgeType && isSrcNodeType) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("edgeType", FloatType, nullable = false),
        StructField("srcType", FloatType, nullable = false)
      ))
    } else if (isEdgeType && isDstNodeType) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("edgeType", FloatType, nullable = false),
        StructField("dstType", FloatType, nullable = false)
      ))
    } else if (isSrcNodeType && isDstNodeType) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("srcType", FloatType, nullable = false),
        StructField("dstType", FloatType, nullable = false)
      ))
    } else if (isEdgeType) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("edgeType", FloatType, nullable = false)
      ))
    } else if (isSrcNodeType) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("srcType", FloatType, nullable = false)
      ))
    } else if (isDstNodeType) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("dstType", FloatType, nullable = false)
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

  def loadString(input: String, index: Int = 0): RDD[String] = {
    val ss = SparkSession.builder().getOrCreate()
    ss.sparkContext.textFile(input)
  }

  def loadStringWeight(input: String, isWeighted: Boolean,
                       srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
                       sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    val schema = if (isWeighted) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("weight", StringType, nullable = false)
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

  // user-item biGraph where the item ids are of string type
  def loadBiGraph(input: String, isWeighted: Boolean,
                  srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
                  sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    val schema = if (isWeighted) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", StringType, nullable = false),
        StructField("weight", FloatType, nullable = false)
      ))
    } else {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", StringType, nullable = false)
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

  def loadNodeSeed(input: String, nodeIndex: Int = 0, sep: String = ""): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val se1 = Array(StructField("node", LongType, nullable = false))
    val schema = StructType(se1.toSeq)
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

  // load string type edge
  def loadStringEdge(input: String, isWeighted: Boolean,
                     srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
                     sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    val schema = if (isWeighted) {
      StructType(Seq(
        StructField("src", StringType, nullable = false),
        StructField("dst", StringType, nullable = false),
        StructField("weight", FloatType, nullable = false)
      ))
    } else {
      StructType(Seq(
        StructField("src", StringType, nullable = false),
        StructField("dst", StringType, nullable = false)
      ))
    }
    ss.read
      .option("sep", sep)
      .option("header", "false")
      .schema(schema)
      .csv(input)
  }


  // load string2int map which was saved in the former string2int reindex process
  def loadString2IntMap(input: String, srcIndex: Int = 0, dstIndex: Int = 1, sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val schema = StructType(Seq(
      StructField("src", StringType, nullable = false),
      StructField("dst", IntegerType, nullable = false)
    ))
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

  def appendSave(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    df.write
      .mode(SaveMode.Append)
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

  def loadEdgesWithStringNodeId(dataset: Dataset[_],
                                srcNodeIdCol: String,
                                dstNodeIdCol: String,
                                needReplicateEdges: Boolean = false
                               ): RDD[(String, String)] = {
    val edges =
      dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getString(0), row.getString(1)))
        .filter(f => f._1 != f._2)

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
    }
    else {
      edges
    }
  }

  def loadEdgesWithIntNodeId(dataset: Dataset[_],
                             srcNodeIdCol: String,
                             dstNodeIdCol: String,
                             needReplicateEdges: Boolean = false
                            ): RDD[(Int, Int)] = {
    val edges =
      dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getString(0).toInt, row.getString(1).toInt))
        .filter(f => f._1 != f._2)

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
    }
    else {
      edges
    }
  }

  def loadEdgesWithWeightStringNodeId(dataset: Dataset[_],
                                      srcNodeIdCol: String,
                                      dstNodeIdCol: String,
                                      weightCol: String,
                                      hasWeight: Boolean = true,
                                      needReplicateEdges: Boolean = false
                                     ): RDD[(String, String, Float)] = {
    val edges =
      if (hasWeight) {
        dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getString(0), row.getString(1), row.getFloat(2)))
          .filter(f => f._1 != f._2 && f._3 != 0.0f)
      } else {
        dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getString(0), row.getString(1), 1.0f))
          .filter(f => f._1 != f._2)
      }

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2, f._3), (f._2, f._1, f._3)))
    }
    else {
      edges
    }
  }

  def loadEdgesWithWeight(dataset: Dataset[_],
                          srcNodeIdCol: String,
                          dstNodeIdCol: String,
                          weightCol: String,
                          hasWeight: Boolean = true,
                          needReplicateEdges: Boolean = false
                         ): RDD[(Long, Long, Float)] = {
    val edges =
      if (hasWeight) {
        dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
          .filter(f => f._1 != f._2 && f._3 != 0.0f)
      } else {
        dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getLong(0), row.getLong(1), 1.0f))
          .filter(f => f._1 != f._2)
      }

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2, f._3), (f._2, f._1, f._3)))
    }
    else {
      edges
    }
  }

  def loadEdges(dataset: Dataset[_],
                srcNodeIdCol: String,
                dstNodeIdCol: String,
                needReplicateEdges: Boolean = false): RDD[(Long, Long)] = {
    val edges = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
    }
    else {
      edges
    }

  }

  def loadEdgesFromDF(dataset: Dataset[_],
                      srcNodeIdCol: String,
                      dstNodeIdCol: String,
                      needReplicateEdges: Boolean = false): RDD[(Long, Long)] = {
    loadEdges(dataset, srcNodeIdCol, dstNodeIdCol, needReplicateEdges)
  }

  def loadEdgesWithWeightFromDF(dataset: Dataset[_],
                                srcNodeIdCol: String,
                                dstNodeIdCol: String,
                                weightCol: String,
                                hasWeight: Boolean = true,
                                needReplicateEdges: Boolean = false): RDD[(Long, Long, Float)] = {
    loadEdgesWithWeight(dataset, srcNodeIdCol, dstNodeIdCol, weightCol, hasWeight, needReplicateEdges)
  }
}