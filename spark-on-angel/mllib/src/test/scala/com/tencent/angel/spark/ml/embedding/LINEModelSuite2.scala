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

import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}
import com.tencent.angel.spark.ml.embedding.line2.LINEModel
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class LINEModelSuite2 extends PSFunSuite with SharedPSContext {
  private val LOCAL_FS = FileSystem.DEFAULT_FS
  private val TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp")

  val input = "../../data/bc/edge"
  val oldOutput = null
  val output = LOCAL_FS + TMP_PATH + "/linemodel_v2"
  //val tmpPath = "file:///E://temp"
  val numPartition = 1
  val lr = 0.025f
  val dim = 4
  val batchSize = 1024
  val numPSPart = 1
  val numEpoch = 5
  val negative = 5
  val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
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
    param.setNumEpoch(numEpoch)
    param.setNegSample(negative)
    param.setMaxIndex(maxNodeId)
    param.setModelCPInterval(1)
    param.setModelSaveInterval(1)
  }

  test("first order") {
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

    if(oldOutput == null) {
      model.randomInitialize(param.seed)
    } else {
      model.load(oldOutput)
    }

    model.train(data, param, output)
    model.save(output, 0)
    model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }

  test("seconds order") {
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
    model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }
}
