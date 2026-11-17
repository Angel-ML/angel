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

import com.tencent.angel.spark.ml.graph.algo.embedding.word2vec.{Word2VecModel, Word2VecParam}
import com.tencent.angel.spark.ml.feature.{Features, SubSampling}
import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class Word2VecModelSuite extends PSFunSuite with SharedPSContext {

  val input = testDataPath("text8/text8.split.head")
  val output = testOutputPath("word2vec")
  val stepSize = 0.1f
  val decayRate = 0.5f
  val embeddingDim = 32
  val batchSize = 50
  val psPartitionNum = 2
  val numEpoch = 2
  val numNegSamples = 5
  val windowSize = 3
  val checkpointInterval=5
  val saveModelInterval=2
  var param: Word2VecParam = _
  var docs: RDD[Array[Int]] = _


  override def beforeAll(): Unit = {
    super.beforeAll()
    val data = sc.textFile(input)
    data.cache()
    val (corpus, _) = Features.corpusStringToInt(sc.textFile(input))
    val subsampleTmp = SubSampling.sampling(corpus)
    docs = subsampleTmp._2.repartition(2)
    docs.cache()
    docs.count()
    data.unpersist()
    val numDocs = docs.count()
    val maxWordId = subsampleTmp._1
    val numTokens = docs.map(_.length).sum().toLong
    val maxLength = docs.map(_.length).max()
    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens maxLength=$maxLength")

    param = new Word2VecParam()
      .setLearningRate(stepSize)
      .setDecayRate(decayRate)
      .setEmbeddingDim(embeddingDim)
      .setBatchSize(batchSize)
      .setWindowSize(windowSize)
      .setNumPSPart(Some(psPartitionNum))
      .setSeed(Random.nextInt())
      .setNumEpoch(numEpoch)
      .setNegSample(numNegSamples)
      .setMaxIndex(maxWordId)
      .setNumRowDataSet(numDocs)
      .setMaxLength(maxLength)
      .setModelCPInterval(checkpointInterval)
      .setModelSaveInterval(saveModelInterval)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("skipgram") {
    val model = new Word2VecModel(param) {
      val messages = new ArrayBuffer[String]()

      // mock logs
      override def logTime(msg: String): Unit = {
        if (null != messages) {
          messages.append(msg)
        }
        println(msg)
      }
    }
    model.randomInitialize(42)
    model.train(docs, param)
    model.save(output, 0, false)

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ").find(_.startsWith("loss=")).get.stripPrefix("loss=").toFloat
    }
    assert(loss.size == numEpoch, s"Expected $numEpoch epoch losses, got ${loss.size}")
    assert(loss.forall(v => !v.isNaN && !v.isInfinity))

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }
}
