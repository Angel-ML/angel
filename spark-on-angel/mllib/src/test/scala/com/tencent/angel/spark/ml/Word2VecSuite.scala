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


package com.tencent.angel.spark.ml

import com.tencent.angel.spark.ml.embedding.Param
import com.tencent.angel.spark.ml.embedding.word2vec.{Word2VecModel, Word2vecWorker}
import com.tencent.angel.spark.ml.feature.Features
import com.tencent.angel.spark.ml.psf.embedding.bad._

import scala.util.Random

class Word2VecSuite extends PSFunSuite with SharedPSContext {

  private val input = "../../data/text8/text8.split.head"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("trainWithServer") {

    val data = sc.textFile(input)
    data.cache()

    val (corpus, _) = Features.corpusStringToInt(sc.textFile(input))
    val docs = corpus.repartition(2)

    docs.cache()
    docs.count()

    data.unpersist()

    val numDocs = docs.count()
    val maxWordId = docs.map(_.max).max().toLong + 1
    val numTokens = docs.map(_.length).sum().toLong
    val maxLength = docs.map(_.length).max()

    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens")

    val param = new Param()
    param.setLearningRate(0.1f)
    param.setEmbeddingDim(100)
    param.setWindowSize(6)
    param.setBatchSize(128)
    param.setSeed(Random.nextInt())
    param.setNumPSPart(Some(2))
    param.setNumEpoch(10)
    param.setNegSample(5)
    param.setMaxIndex(maxWordId)
    param.setMaxLength(maxLength)
    param.setModel("cbow")
    param.setModelCPInterval(100)

    val model = new Word2VecModel(param)
    model.train(docs, param, "model")
  }

  test("trainWithWorker") {
    val numNode = 100
    val dimension = 10
    val numNodePerRow = 10
    val modelType = "cbow"
    val model = new Word2vecWorker(numNode, dimension, modelType, 10, numNodePerRow)

    val matrix = model.matrix

    matrix.psfUpdate(new W2VRandom(new W2VRandomParam(matrix.id, dimension)));

    val indices = Array(1, 3, 50, 29, 60)
    val param = new W2VPullParam(matrix.id, indices, numNodePerRow, dimension)
    val func  = new W2VPull(param)
    val result = matrix.psfGet(func).asInstanceOf[W2VPullResult]
    assert(result.layers.length == indices.length * dimension * 2)

    for (idx <- 0 until indices.length) {
      for (i <- 0 until dimension)
        assert(result.layers(i + idx * dimension) != 0.0f)
      for (i <- dimension until dimension * 2) {
        assert(result.layers(i + idx * dimension) == 0.0f)
      }
    }
  }
}
