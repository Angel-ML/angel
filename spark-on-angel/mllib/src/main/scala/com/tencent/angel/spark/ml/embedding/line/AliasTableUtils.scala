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

import java.util.Random

/**
  * Alias table tools
  */
object AliasTableUtils {
  /**
    * Build alias table use weights
    *
    * @param weights edge weights
    * @return alias table
    */
  def buildAliasTable(weights: Array[Float]): (Array[Float], Array[Int]) = {
    // Get weight sum
    var sum = 0.0
    weights.foreach(f => sum += f)

    val prob = new Array[Float](weights.length)
    val alias = new Array[Int](weights.length)

    // Normalize
    val edgeNum = weights.length
    var smallNum = 0
    var bigNum = 0

    for (index <- weights.indices) {
      weights(index) = (weights(index) * edgeNum / sum).toFloat
      if (weights(index) < 1.0) {
        smallNum += 1
      } else {
        bigNum += 1
      }
    }

    val bigEdges = new Array[Int](bigNum)
    val smallEdges = new Array[Int](smallNum)
    var bigIndex = 0
    var smallIndex = 0

    for (index <- weights.indices.reverse) {
      if (weights(index) < 1) {
        smallEdges(smallIndex) = index
        smallIndex += 1
      } else {
        bigEdges(bigIndex) = index
        bigIndex += 1
      }
    }

    var currentBigEdgeIndex = 0
    var currentSmallEdgeIndex = 0

    while (bigIndex > 0 && smallIndex > 0) {
      bigIndex -= 1
      smallIndex -= 1
      currentBigEdgeIndex = bigEdges(bigIndex)
      currentSmallEdgeIndex = smallEdges(smallIndex)

      prob(currentSmallEdgeIndex) = weights(currentSmallEdgeIndex)
      alias(currentSmallEdgeIndex) = currentBigEdgeIndex

      weights(currentBigEdgeIndex) = weights(currentBigEdgeIndex) + weights(currentSmallEdgeIndex) - 1

      if (weights(currentBigEdgeIndex) < 1.0) {
        smallEdges(smallIndex) = currentBigEdgeIndex
        smallIndex += 1
      } else {
        bigEdges(bigIndex) = currentBigEdgeIndex
        bigIndex += 1
      }
    }

    while (bigIndex > 0) {
      bigIndex -= 1
      prob(bigEdges(bigIndex)) = 1.0f
    }

    while (smallIndex > 0) {
      smallIndex -= 1
      prob(smallEdges(smallIndex)) = 1.0f
    }

    (prob, alias)
  }

  /**
    * Build alias table use weights
    *
    * @param weights edge weights
    * @return alias table
    */
  def buildAliasTable(weights: Array[Double]): (Array[Double], Array[Int]) = {
    // Get weight sum
    var sum = 0.0
    weights.foreach(f => sum += f)

    val prob = new Array[Double](weights.length)
    val alias = new Array[Int](weights.length)

    // Normalize
    val edgeNum = weights.length
    var smallNum = 0
    var bigNum = 0

    for (index <- weights.indices) {
      weights(index) = weights(index) * edgeNum / sum
      if (weights(index) < 1.0) {
        smallNum += 1
      } else {
        bigNum += 1
      }
    }

    val bigEdges = new Array[Int](bigNum)
    val smallEdges = new Array[Int](smallNum)
    var bigIndex = 0
    var smallIndex = 0

    for (index <- weights.indices.reverse) {
      if (weights(index) < 1) {
        smallEdges(smallIndex) = index
        smallIndex += 1
      } else {
        bigEdges(bigIndex) = index
        bigIndex += 1
      }
    }

    var currentBigEdgeIndex = 0
    var currentSmallEdgeIndex = 0

    while (bigIndex > 0 && smallIndex > 0) {
      bigIndex -= 1
      smallIndex -= 1
      currentBigEdgeIndex = bigEdges(bigIndex)
      currentSmallEdgeIndex = smallEdges(smallIndex)

      prob(currentSmallEdgeIndex) = weights(currentSmallEdgeIndex)
      alias(currentSmallEdgeIndex) = currentBigEdgeIndex

      weights(currentBigEdgeIndex) = weights(currentBigEdgeIndex) + weights(currentSmallEdgeIndex) - 1

      if (weights(currentBigEdgeIndex) < 1.0) {
        smallEdges(smallIndex) = currentBigEdgeIndex
        smallIndex += 1
      } else {
        bigEdges(bigIndex) = currentBigEdgeIndex
        bigIndex += 1
      }
    }

    while (bigIndex > 0) {
      bigIndex -= 1
      prob(bigEdges(bigIndex)) = 1.0f
    }

    while (smallIndex > 0) {
      smallIndex -= 1
      prob(smallEdges(smallIndex)) = 1.0f
    }

    for (i <- weights.indices) {
      println(s"i=$i, prob=${prob(i)}, alias=${alias(i)}")
    }

    (prob, alias)
  }

  /**
    * Batch sampling from alias table
    *
    * @param rand      random
    * @param prob      prob array
    * @param alias     alias array
    * @param sampleNum sample number
    * @return samples
    */
  def batchSample(rand: Random, prob: Array[Float], alias: Array[Int], sampleNum: Int): Array[Int] = {
    val indices = new Array[Int](sampleNum)
    for (i <- 0 until sampleNum) {
      val id = rand.nextInt(prob.length)
      val v = rand.nextDouble().toFloat
      if (v < prob(id)) {
        indices(i) = id
      } else {
        indices(i) = alias(id)
      }
    }
    indices
  }

  /**
    * Batch sampling from alias table
    *
    * @param rand      random
    * @param prob      prob array
    * @param alias     alias array
    * @param sampleNum sample number
    * @return samples
    */
  def batchSample(rand: Random, prob: Array[Double], alias: Array[Int], sampleNum: Int): Array[Int] = {
    val indices = new Array[Int](sampleNum)
    for (i <- 0 until sampleNum) {
      val id = rand.nextInt(prob.length)
      val v = rand.nextDouble()
      if (v < prob(id)) {
        indices(i) = id
      } else {
        indices(i) = alias(id)
      }
    }
    indices
  }
}
