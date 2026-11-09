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

package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition.String2IntReIndex._
import com.tencent.angel.spark.ml.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object String2IntReIndexExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val rawInput = params.getOrElse("rawInput", null)
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.TAB))
    val encodedPath = params.getOrElse("encodedPath", null)
    val decodedPath = params.getOrElse("decodedPath", null)
    val mapPath = params.getOrElse("mapPath", null)
    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean

    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt

    val method = params.getOrElse("method", "encode")

    start(mode)
    val startTime = System.currentTimeMillis()

    if (method == "encode") {
      encode(rawInput, sep, isWeighted, srcIndex, dstIndex, weightIndex, mapPath, encodedPath, partitionNum)
    } else {
      decode(encodedPath, sep, mapPath, decodedPath)
    }

    println(s"cost ${System.currentTimeMillis() - startTime} ms.")
    stop()
  }

  def encode(rawInput: String, sep: String, isWeighted: Boolean, srcIndex: Int, dstIndex: Int, weightIndex: Int,
             mapPath: String, encodedPath: String, partitionNum: Int): Unit = {
    val rawEdges = if (isWeighted) {
      GraphIO.loadStringEdge(rawInput, isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
        .select("src", "dst", "weight").rdd
        .map(x => (x.getString(0), x.getString(1), x.getFloat(2)))
        .repartition(partitionNum)
    } else {
      GraphIO.loadStringEdge(rawInput, isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
        .select("src", "dst").rdd
        .map(x => (x.getString(0), x.getString(1), 1.0f))
        .repartition(partitionNum)
    }
    val encoded = encodeEdge(rawEdges, mapPath)
    val schema = StructType(Seq(
      StructField(s"src", IntegerType, nullable = false),
      StructField(s"dst", IntegerType, nullable = false),
      StructField(s"weight", FloatType, nullable = false)
    ))
    val ss = SparkSession.builder().getOrCreate()
    GraphIO.save(ss.createDataFrame(encoded.map(x => Row(x._1, x._2, x._3)), schema), encodedPath)
  }

  def decode(encodedPath: String, sep: String, mapPath: String, decodedPath: String): Unit = {
    val rawEdges = GraphIO.loadInt(encodedPath, isWeighted = true, sep = sep)
      .select("src", "dst", "weight").rdd
      .map(x => (x.getInt(0), x.getInt(1), x.getFloat(2)))
    val decoded = decodeEdge(rawEdges, mapPath)
    val schema = StructType(Seq(
    StructField(s"src", StringType, nullable = false),
    StructField(s"dst", StringType, nullable = false),
    StructField(s"weight", FloatType, nullable = false)
    ))
    val ss = SparkSession.builder().getOrCreate()
    GraphIO.save(ss.createDataFrame(decoded.map(x => Row(x._1, x._2, x._3)), schema), decodedPath)
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    conf.setMaster(mode)
    conf.setAppName("Test")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

}
