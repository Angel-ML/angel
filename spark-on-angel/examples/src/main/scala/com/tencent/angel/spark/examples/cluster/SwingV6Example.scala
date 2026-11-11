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

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.rank.swing.Swing
import com.tencent.angel.spark.ml.graph.utils.{Delimiter, GraphIO}
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

/*
if item ids are strings, set reindexItem param to true
to execute reindexing and decoding procedures.
*/

object SwingV6Example {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.TAB))
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "1")).toInt
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val alpha = params.getOrElse("alpha", "0").toFloat
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)
    val topFrom = params.getOrElse("topFrom", "1").toInt
    val topTo = params.getOrElse("topTo", "1").toInt
    val beta = params.getOrElse("beta", "5").toInt
    val gamma = params.getOrElse("gamma", "-0.3").toFloat
    val superItemThreshold = params.getOrElse("superItemThreshold", "30").toInt
    val superItemPairBatch = params.getOrElse("superItemPairBatch", "10").toInt
    val reindexItem = params.getOrElse("reindexItem", "false").toBoolean

    start(mode)
    val startTime = System.currentTimeMillis()
    val swing = new Swing()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setPSPartitionNum(psPartitionNum)
      .setDelta(alpha)
      .setBeta(beta)
      .setGamma(gamma)
      .setSuperItemThreshold(superItemThreshold)
    swing.setItemFrom(topFrom)
    swing.setItemTo(topTo)
    swing.setSuperItemPairBatch(superItemPairBatch)

    if (reindexItem) {
      // read string type item id
      val df_ = GraphIO.loadBiGraph(input, isWeighted = false,
        srcIndex = srcIndex, dstIndex = dstIndex, sep = sep)
      val items = df_.select("dst").rdd
        .map(Row => Row.getString(0))
        .filter(_.nonEmpty)
        .distinct()
        .zipWithIndex
        .collect()
      println(s"num valid items: ${items.length}")
      val string2long = items.toMap

      // reindex
      val edges = df_.select("src", "dst").rdd
        .filter(row => !row.anyNull)
        .map (row => (row.getLong(0), row.getString(1).trim))
        .filter(_._2.nonEmpty)
        .distinct(partitionNum)
        .map (x => Row.fromSeq(Seq[Any](x._1, string2long(x._2))))
      val schema = StructType(Seq(
        StructField(s"${"src"}", LongType, nullable = false),
        StructField(s"${"dst"}", LongType, nullable = false)
      ))
      val df = df_.sparkSession.createDataFrame(edges, schema)

      //collect app name and data size
      DataStatistics.setAppName(swing.getClass.getCanonicalName)
      DataStatistics.calcRDDSize(df, "angel.input.data")

      // compute and decode
      val mapping_ = swing.transform(df)
      val long2string = items.map(f => (f._2, f._1)).toMap
      val reSchema = StructType(Seq(
        StructField(s"${"itemI"}", StringType, nullable = false),
        StructField(s"${"itemJ"}", StringType, nullable = false),
        StructField(s"${"score"}", FloatType, nullable = false)
      ))
      val re = mapping_.select("itemI", "itemJ", "score").rdd
        .map(row => Row.fromSeq(Seq[Any](long2string(row.getLong(0)), long2string(row.getLong(1)), row.getFloat(2))))
      val mapping = mapping_.sparkSession.createDataFrame(re, reSchema)
      GraphIO.save(mapping, output) // save
    } else {
      val df = GraphIO.load(input, isWeighted = false,
        srcIndex = srcIndex, dstIndex = dstIndex, sep = sep)

      //collect app name and data size
      DataStatistics.setAppName(swing.getClass.getCanonicalName)
      DataStatistics.calcRDDSize(df, "angel.input.data")
      DataStatistics.setDataPartitionNum(partitionNum)
      DataStatistics.setPSPartitionNum(psPartitionNum)

      val mapping = swing.transform(df)
      GraphIO.save(mapping, output)
    }

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    val executorJvmOptions = "-Djava.library.path=$JAVA_LIBRARY_PATH " +
      "-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:MaxPermSize=256m -XX:SurvivorRatio=4 -XX:+UseMembar -XX:+UseConcMarkSweepGC " +
      "-XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 " +
      "-XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled " +
      "-XX:CMSInitiatingOccupancyFraction=50 -XX:+UseCompressedOops"

    println(s"executorJvmOptions = $executorJvmOptions")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)

    conf.setMaster(mode)
    conf.setAppName("swing")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
