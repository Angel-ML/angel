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


package com.tencent.angel.spark.ml.embedding.line

import scala.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.embedding.Param

object LINE {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val vectorDim = params.getOrElse("vectorDim", "10").toInt
    val negSample = params.getOrElse("negSample", "5").toInt
    val maxEpoch = params.getOrElse("maxEpoch", "10").toInt
    val learningRate = params.getOrElse("learningRate", "0.1").toFloat
    val numPSPart = params.get("numPSPart").map(_.toInt)
    val nodesNumPerRow = params.get("nodesNumPerRow").map(_.toInt)
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val modelPath = params.getOrElse("modelPath", null)
    val modelCPInterval = params.getOrElse("modelCPInterval", Int.MaxValue.toString).toInt
    val order = params.get("order").fold(2)(_.toInt)
    val validSet = params.get("validSet").filter(_.nonEmpty)
    val seed = params.get("seed").filter(_.nonEmpty).map(_.toInt).getOrElse(Random.nextInt)

    val ss = SparkSession.builder()
      .master(mode)
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    if (ss.sparkContext.isLocal)
      ss.sparkContext.setLogLevel("WARN")

    PSContext.getOrCreate(ss.sparkContext)

    val lineParam = new Param()
      .setPartitionNum(partitionNum)
      .setEmbeddingDim(vectorDim)
      .setNegSample(negSample)
      .setNumEpoch(maxEpoch)
      .setLearningRate(learningRate)
      .setModelPath(modelPath)
      .setModelCPInterval(modelCPInterval)
      .setOrder(order)
      .setBatchSize(batchSize)
      .setNodesNumPerRow(nodesNumPerRow)
      .setSeed(seed)
      .setNumPSPart(numPSPart.orElse(ss.conf.getOption("spark.ps.instances").map(_.toInt)))


    val edges = ss.sparkContext
      .textFile(input, partitionNum)
      .sample(withReplacement = false, sampleRate)
      .mapPartitions { lines =>
        val r = Random
        lines.map { line =>
          val arr = line.split("[\\s|,]+")
          val src = arr(0).toInt
          val dst = arr(1).toInt
          (r.nextInt(partitionNum), (src, dst))
        }
      }.repartition(partitionNum)
      .values
      .persist(StorageLevel.MEMORY_AND_DISK)

    val validData = validSet.map { path =>
      ss.sparkContext.textFile(path, partitionNum).map { line =>
        val arr = line.split("[\\s|,]+")
        (arr(0).toInt, arr(1).toInt)
      }.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val numVertices = edges.map(x => math.max(x._1, x._2)).max() + 1
    val numEdge = edges.count()
    println(s"num lines in raw docs: $numEdge")
    println(s"num vertices: $numVertices")
    lineParam.setMaxIndex(numVertices)
      .setNumRowDataSet(numEdge)


    new LINEModel(lineParam).train(edges, lineParam, validData)
  }
}