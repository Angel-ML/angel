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

import com.tencent.angel.spark.ml.embedding.line2.LINE
import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class LINEModelSuite2 extends PSFunSuite with SharedPSContext {
  val input = "../../data/bc/part-00000"
  val output = "file:///E://model_new/"
  val oldOutput = null//"file:///E:\\temp\\application_1565577700269_-1030306181_2e9fbfff-8f3c-41f4-b015-59490ef6daf5\\snapshot\\2"
  val tmpPath = "file:///E://temp"
  val numPartition = 1
  val lr = 0.025f
  val dim = 32
  val batchSize = 1024
  val numPSPart = 2
  val numEpoch = 10
  val negative = 5
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

    val isWeight = false
    val srcIndex = 0
    val dstIndex = 1
    val weightIndex = 2
    val useBalancePartition = false
    val sep = " "

    val edges = load(input, true, sep = sep)

    val model = new LINE()
    model
      .setEmbedding(dim)
      .setNegative(negative)
      .setStepSize(lr)
      .setOrder(2)
      .setEpochNum(numEpoch)
      .setBatchSize(batchSize)
      .setPartitionNum(numPartition)
      .setPSPartitionNum(numPSPart)
      .setOutput(output)
      .setSaveModelInterval(1)
      .setCheckpointInterval(1)
      .setIsWeighted(true)
      //.setRemapping(true)

    model.transform(edges)

    model.save(output, numEpoch)
  }

  def load(input:String, isWeighted:Boolean, sep: String = " ") = {
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


  /*test("first order") {
    param.setOrder(1)
    val model = new LINEModel(param) {
      val messages = new ArrayBuffer[String]()

      // mock logs
      override def logTime(msg: String): Unit = {
        if (null != messages) {
          messages.append(msg)
        }
        println(msg)
      }
    }
    model.train(data, param, "")
    model.save(output, 0)
    //model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }*/

  /*test("seconds order") {

    /*val path = new Path(output, "test")
    val fs = path.getFileSystem(new Configuration())
    val stream = fs.create(path, true, 10000000)

    var startTs = System.currentTimeMillis()
    for(i <- 0 until 10000000) {
      stream.writeFloat(i.toFloat)
    }
    println(s"write int use time=${System.currentTimeMillis() - startTs}")

    startTs = System.currentTimeMillis()
    for(i <- 0 until 10000000) {
      stream.writeFloat(i.toFloat)
    }
    println(s"write int use time=${System.currentTimeMillis() - startTs}")

    startTs = System.currentTimeMillis()
    val data = new Array[Byte](10000000 * 4)
    for(i <- 0 until 10000000) {
      val intValue = java.lang.Float.floatToIntBits(i)
      val index = i << 2
      data(index + 0) = ((intValue >>> 24) & 0xFF).toByte
      data(index + 1) = ((intValue >>> 16) & 0xFF).toByte
      data(index + 2) = ((intValue >>> 8) & 0xFF).toByte
      data(index + 3) = ((intValue) & 0xFF).toByte
    }
    stream.write(data)
    println(s"write int use time=${System.currentTimeMillis() - startTs}")
    */

    param.setOrder(2)
    val model = new LINEModel(param) {
      val messages = new ArrayBuffer[String]()

      // mock logs
      override def logTime(msg: String): Unit = {
        if (null != messages) {
          messages.append(msg)
        }
        println(msg)
      }
    }

    if(oldOutput == null) {
      model.randomInitialize(param.seed)
    } else {
      model.load(oldOutput)
    }

    model.train(data, param, output)
    model.save(output, param.numEpoch)
    //model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }*/
}
