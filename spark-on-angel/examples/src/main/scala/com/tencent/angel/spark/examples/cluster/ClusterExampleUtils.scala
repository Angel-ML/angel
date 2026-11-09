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

import java.util.regex.Pattern

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Shared helpers for the graph cluster examples.
 */
object ClusterExampleUtils {

  /**
   * Create the SparkContext and start Angel PS.
   *
   * The master is taken from spark-submit (e.g. `--master yarn --deploy-mode cluster`) unless the
   * "mode" argument is given explicitly.
   */
  def start(params: Map[String, String], appName: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    params.get("mode").filter(_.nonEmpty).foreach(mode => conf.setMaster(mode))

    val executorJvmOptions = conf.get("spark.executor.extraJavaOptions", "")
    conf.set("spark.executor.extraJavaOptions",
      s"$executorJvmOptions -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -Xss4M".trim)
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")

    val sc = new SparkContext(conf)
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new IllegalArgumentException("checkpoint dir not provided, set cpDir:<path>"))
    sc.setCheckpointDir(cpDir)

    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def required(params: Map[String, String], key: String): String = {
    params.get(key).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"$key is required, set $key:<value>"))
  }

  def parseSep(params: Map[String, String], key: String = "sep", default: String = "space"): String = {
    params.getOrElse(key, default) match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "|"
      case other => throw new IllegalArgumentException(
        s"unsupported $key: $other, use one of space/comma/tab/colon/bar")
    }
  }

  def psPartitionNum(params: Map[String, String], sc: SparkContext, default: String = "10"): Int = {
    params.getOrElse("psPartitionNum", sc.getConf.get("spark.ps.instances", default)).toInt
  }

  /**
   * Load edges with string node ids, as required by the embedding models that remap node ids
   * themselves (LINE v2/v3, BGE). The result has columns "src", "dst" (StringType) and, if
   * weighted, "weight" (FloatType). Blank lines and lines starting with "#" are skipped.
   */
  def loadStringEdges(input: String, sep: String, isWeighted: Boolean, srcIndex: Int = 0,
      dstIndex: Int = 1, weightIndex: Int = 2): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val splitter = if (sep == " ") "\\s+" else Pattern.quote(sep)
    val maxIndex = if (isWeighted) math.max(math.max(srcIndex, dstIndex), weightIndex)
    else math.max(srcIndex, dstIndex)

    val rows = ss.sparkContext.textFile(input)
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .map(_.split(splitter))
      .filter(_.length > maxIndex)
      .map { fields =>
        if (isWeighted) Row(fields(srcIndex), fields(dstIndex), fields(weightIndex).toFloat)
        else Row(fields(srcIndex), fields(dstIndex))
      }

    val fields = Seq(StructField("src", StringType, nullable = false),
      StructField("dst", StringType, nullable = false)) ++
      (if (isWeighted) Seq(StructField("weight", FloatType, nullable = false)) else Seq.empty)
    ss.createDataFrame(rows, StructType(fields))
  }
}
