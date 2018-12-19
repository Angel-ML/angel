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

package com.tencent.angel.spark.ml.feature

import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.IntIntVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import it.unimi.dsi.fastutil.ints.{Int2DoubleOpenHashMap, IntOpenHashSet}
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * This class performs the sub-sampling process for a given corpus dataset.
  * We here follow the method of Google word2vec code to determine the sampling rate.
  * The probability for a word ``w`` to be kept in the corpus is
  *    p(w) = (\sqrt(z(w)/0.001) + 1) * (0.001/z(w))
  */
object SubSampling {

  /**
    * return (maxWordId, dataset)
    * @param data
    * @return
    */
  def sampling(data: RDD[Array[Int]]): (Long, RDD[Array[Int]]) = {
    // First, calculate z(w) for each word
    val numTokens = data.map(sentence => sentence.length).sum()
    val denseDim  = data.flatMap(sentence => sentence).max() + 1
    val freqRDD = data.flatMap(sentence => sentence).map(w => (w, 1)).reduceByKey(_ + _)

    // Create a matrix on server to maintain the frequence for each word
    val freqMatrix = PSMatrixUtils.createPSMatrixCtx("freq", 1, denseDim, RowType.T_INT_DENSE)
    val freqMatrixId = PSMatrixUtils.createPSMatrix(freqMatrix)

    // Function to initialize the frequence matrix
    def initializeFrequenceMatrix(iterator: Iterator[(Int, Int)],
                                  denseDim: Int,
                                  freqMatrixId: Int): Iterator[Int] = {
      // Get new matrix meta for the freqMatrixId
      PSContext.instance().refreshMatrix
      val update = VFactory.sparseIntVector(denseDim)
      while (iterator.hasNext) {
        val (word, freq) = iterator.next()
        update.set(word, freq)
      }

      PSMatrixUtils.incrementRow(freqMatrixId, 0, update)
      Iterator.single(0)
    }

    // Do initialize for frequence matrix
    freqRDD.mapPartitions(it =>
      initializeFrequenceMatrix(it,
        denseDim,
        freqMatrixId),
      true).count()

    // Function for sampling, where ``numToken`` is the total number of tokens in
    // the corpus dataset
    def doSampling(iterator: Iterator[Array[Int]],
                   numTokens: Double,
                   freqMatrixId: Int): Iterator[Array[Int]] = {

      PSContext.instance().refreshMatrix
      val tokens = iterator.toArray

      // First, build puling index to pull the frequency for each word
      val pullIndices = new IntOpenHashSet()
      var i = 0
      while (i < tokens.length) {
        tokens(i).foreach(t => pullIndices.add(t))
        i += 1
      }

      val indices = VFactory.denseIntVector(pullIndices.toIntArray())
      val freqIndex = PSMatrixUtils.getRowWithIndex(1, freqMatrixId, 0, indices)
        .asInstanceOf[IntIntVector]

      // Second, calculate probability for each word
      val zw = new Int2DoubleOpenHashMap()
      val it = freqIndex.getStorage.entryIterator()
      while (it.hasNext) {
        val entry = it.next()
        val key = entry.getIntKey
        val value = entry.getIntValue.toDouble / numTokens
        val p = (math.sqrt(value / 0.001) + 1) * (0.001 / value)
        zw.put(key, p)
      }

      val rand = new Random(System.currentTimeMillis())

      i = 0
      while (i < tokens.length) {
        var j = 0
        val sentence = tokens(i)
        while (j < sentence.length) {
          val p = zw.get(sentence(j))
          // random peaking to decide whether ignore current word or not
          if (rand.nextDouble() >= p)
            sentence(j) = -1
          j += 1
        }

        tokens(i) = sentence.filter(w => w != -1)
        i += 1
      }

      tokens.iterator
    }

    // Do sampling
    val samples = data.mapPartitions(it =>
      doSampling(it,
        numTokens,
        freqMatrixId),
      true)

    (denseDim, samples.filter(s => s.nonEmpty).filter(s => s.length > 1))
  }

}
