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

package com.tencent.angel.spark.examples.local

import scala.util.Random

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.Param
import com.tencent.angel.spark.ml.embedding.line.LINEModel

object LINEExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("LINE example")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    PSContext.getOrCreate(sc)

    val input = "data/bc/edge"
    val output = "model/"
    val numPartition = 4
    val storageLevel = StorageLevel.MEMORY_ONLY

    val data = sc.textFile(input).map{line =>
      val arr = line.split(" ")
      val src = arr(0).toInt
      val dst = arr(1).toInt
      (Random.nextInt(numPartition), (src, dst))
    }.repartition(numPartition).values.persist(storageLevel)

    val numEdge = data.count()
    val maxNodeId = data.map{case (src, dst) => math.max(src, dst)}.max().toLong + 1

    println(s"numEdge=$numEdge maxNodeId=$maxNodeId")

    val param = new Param()
    param.setLearningRate(0.025f)
    param.setEmbeddingDim(128)
    param.setBatchSize(128)
    param.setSeed(Random.nextInt())
    param.setNumPSPart(Some(2))
    param.setNumEpoch(30)
    param.setNegSample(5)
    param.setMaxIndex(maxNodeId)
    param.setOrder(2)
    param.setModelCPInterval(1000)

    val model = new LINEModel(param)
    model.train(data, param, "")
    model.save(output, 0)

    PSContext.stop()
    sc.stop()
  }

}
