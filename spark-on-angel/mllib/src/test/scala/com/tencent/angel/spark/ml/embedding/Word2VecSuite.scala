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

import com.tencent.angel.spark.ml.embedding.word2vec.Word2vecWorker
import com.tencent.angel.spark.ml.psf.embedding.bad._
import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}

import scala.util.Random

class Word2VecSuite extends PSFunSuite with SharedPSContext {

  private val input = "../../data/text8/text8.split.head"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("trainWithWorkerPull&Push") {
    val numNode = 100
    val dimension = 10
    val numNodePerRow = 10
    val modelType = "cbow"
    val numPart = 5
    val model = new Word2vecWorker(numNode, dimension, modelType, numPart, numNodePerRow, Random.nextInt())

    val matrix = model.matrix

    var indices = Array(12, 33, 52, 27, 49)
    var param = new W2VPullParam(matrix.id, indices, numNodePerRow, dimension)
    var func  = new W2VPull(param)
    var result = matrix.psfGet(func).asInstanceOf[W2VPullResult]
    assert(result.layers.length == indices.length * dimension * 2)

    for (idx <- 0 until indices.length) {
      for (i <- 0 until dimension)
        assert(result.layers(i + idx * dimension * 2) != 0.0f)
      for (i <- dimension until dimension * 2) {
        assert(result.layers(i + idx * dimension * 2) == 0.0f)
      }
    }

    val deltas = new Array[Float](result.layers.length)
    for (i <- 0 until deltas.length) deltas(i) = 0.1f

    val update = new W2VPush(new W2VPushParam(matrix.id, indices, deltas, numNodePerRow, dimension))
    matrix.psfUpdate(update).get()

    val resultAfterUpdate = matrix.psfGet(func).asInstanceOf[W2VPullResult]

    for (i <- 0 until result.layers.length) {
      assert(result.layers(i) + 0.1f == resultAfterUpdate.layers(i))
    }

    indices = Array(23, 44, 65, 83, 88)
    param = new W2VPullParam(matrix.id, indices, numNodePerRow, dimension)
    func  = new W2VPull(param)
    result = matrix.psfGet(func).asInstanceOf[W2VPullResult]
    assert(result.layers.length == indices.length * dimension * 2)

    for (idx <- 0 until indices.length) {
      for (i <- 0 until dimension)
        assert(result.layers(i + idx * dimension * 2) != 0.0f)
      for (i <- dimension until dimension * 2) {
        assert(result.layers(i + idx * dimension * 2) == 0.0f)
      }
    }
  }

}
