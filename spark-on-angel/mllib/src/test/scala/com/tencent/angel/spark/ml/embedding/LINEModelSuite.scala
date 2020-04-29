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
package com.tencent.angel.spark.ml.embedding

import com.tencent.angel.spark.ml.embedding.line.LINE
import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.util.Random

class LINEModelSuite2 extends PSFunSuite with SharedPSContext {
  private val LOCAL_FS = FileSystem.DEFAULT_FS
  private val TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp")
  val input = "../../data/bc/edge"
  val output = LOCAL_FS + TMP_PATH + "/model/line"
  val numPartition = 1
  val lr = 0.025f
  val dim = 32
  val batchSize = 1024
  val numPSPart = 2
  val numEpoch = 10
  val negative = 5
  val order = 2
  val storageLevel: StorageLevel = StorageLevel.DISK_ONLY
  var param: Param = _
  var data: RDD[(Int, Int)] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = sc.textFile(input).mapPartitions { iter =>
      val r = new Random()
      iter.map { line =>
        val arr = line.split(" ")
        val src = arr(0).toInt
        val dst = arr(1).toInt
        (r.nextInt, (src, dst))
      }
    }.repartition(numPartition).values.persist(storageLevel)
    val numEdge = data.count()
    val maxNodeId = data.map { case (src, dst) => math.max(src, dst) }.max().toLong + 1
    println(s"numEdge=$numEdge maxNodeId=$maxNodeId")


    param = new Param()
    param.setLearningRate(lr)
    param.setEmbeddingDim(dim)
    param.setBatchSize(batchSize)
    param.setNumPSPart(Some(numPSPart))
    param.setMaxIndex(maxNodeId)
    param.setNumEpoch(numEpoch)
    param.setNegSample(negative)
    param.setModelCPInterval(1)
    param.setModelSaveInterval(1)

  }

  test("line_weight") {
    param.setOrder(2)
    val sep = " "

    val edges = load(input, true, sep = sep)

    val model = new LINE()
    model
      .setEmbedding(dim)
      .setNegative(negative)
      .setStepSize(lr)
      .setOrder(order)
      .setEpochNum(numEpoch)
      .setBatchSize(batchSize)
      .setPartitionNum(numPartition)
      .setPSPartitionNum(numPSPart)
      .setOutput(output)
      .setSaveModelInterval(1)
      .setCheckpointInterval(1)
      .setIsWeighted(false)
      .setRemapping(true)

    model.transform(edges)

    model.save(output, numEpoch, false)
  }

  def load(input:String, isWeighted:Boolean, sep: String = " "): DataFrame = {
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
}
