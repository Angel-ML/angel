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


package com.tencent.angel.spark.ml.embedding.word2vec

import scala.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.embedding.Param


object Word2Vec {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val trainSet = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val vectorDim = params.getOrElse("vectorDim", "100").toInt
    val window = params.getOrElse("window", "6").toInt
    val negSample = params.getOrElse("negSample", "5").toInt
    val maxEpoch = params.getOrElse("maxEpoch", "10").toInt
    val learningRate = params.getOrElse("learningRate", "0.1").toFloat
    val numPSPart = params.get("numPSPart").map(_.toInt)
    val nodesNumPerRow = params.get("nodesNumPerRow").map(_.toInt)
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val modelPath = params.getOrElse("modelPath", null)
    val modelCPInterval = params.getOrElse("modelCPInterval", Int.MaxValue.toString).toInt
    val validSet = params.get("validSet").filter(_.nonEmpty)
    val seed = params.get("seed").filter(_.nonEmpty).map(_.toInt).getOrElse(Random.nextInt)

    val ss = SparkSession.builder()
      .master(mode)
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    if (ss.sparkContext.isLocal)
      ss.sparkContext.setLogLevel("WARN")
    PSContext.getOrCreate(ss.sparkContext)

    val w2vParam = new Param()
      .setPartitionNum(partitionNum)
      .setEmbeddingDim(vectorDim)
      .setNegSample(negSample)
      .setWindowSize(window)
      .setNumEpoch(maxEpoch)
      .setLearningRate(learningRate)
      .setModelPath(modelPath)
      .setModelCPInterval(modelCPInterval)
      .setBatchSize(batchSize)
      .setNodesNumPerRow(nodesNumPerRow)
      .setSeed(seed)
      .setNumPSPart(numPSPart.orElse(ss.conf.getOption("spark.ps.instances").map(_.toInt)))


    val trainDocs = ss.sparkContext.textFile(trainSet, partitionNum).map { line =>
      val arr = line.split("[\\s|,]+").map(_.toInt)
      val pid = Random.nextInt(partitionNum)
      (pid, arr)
    }.repartition(partitionNum).values.persist(StorageLevel.MEMORY_AND_DISK)

    val validData = validSet.map {
      ss.sparkContext.textFile(_, partitionNum).map(_.split("[\\s|,]+").map(_.toInt))
        .persist(StorageLevel.MEMORY_AND_DISK)
    }

    val numDocs = trainDocs.count()
    println(s"num lines in raw docs: $numDocs")
    val numWords = trainDocs.map(_.max).max() + 1
    println(s"num words: $numWords")
    w2vParam.setMaxIndex(numWords)
      .setNumRowDataSet(numDocs)

    new Word2VecModel(w2vParam).train(trainDocs, w2vParam, validData)
  }
}