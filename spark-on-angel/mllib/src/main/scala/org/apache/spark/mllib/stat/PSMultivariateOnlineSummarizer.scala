/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

/**
 *
 * This class is a copy of MultivariateOnlineSummarizer.scala in org.apache.spark.mllib.stat
 * package of spark 2.1.0 MLlib.
 *
 * Based on the original version, we improve the algorithm with Angel PS-Service.
 */

package org.apache.spark.mllib.stat

import com.tencent.angel.spark.context.{PSContext, PSVectorPool}
import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.{Vector, Vectors}


/**
 * :: DeveloperApi ::
 * MultivariateOnlineSummarizer implements `MultivariateStatisticalSummary` to compute the mean,
 * variance, minimum, maximum, counts, and nonzero counts for instances in sparse or dense vector
 * format in an online fashion.
 *
 * Two MultivariateOnlineSummarizer can be merged together to have a statistical summary of
 * the corresponding joint dataset.
 *
 * A numerically stable algorithm is implemented to compute the mean and variance of instances:
 * Reference: <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance">
 * variance-wiki</a>
 * Zero elements (including explicit zero values) are skipped when calling add(),
 * to have time complexity O(nnz) instead of O(n) for each column.
 *
 * For weighted instances, the unbiased estimation of variance is defined by the reliability
 * weights:
 * see <a href="https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Reliability_weights">
 * Reliability weights (Wikipedia)</a>.
 */
@Since("1.1.0")
@DeveloperApi
class PSMultivariateOnlineSummarizer(@transient private val psVector: PSVector)
  extends Serializable {

  private val n = psVector.dimension
  private val currMean: PSVector = PSVector.duplicate(psVector)
  private val currM2n: PSVector = PSVector.duplicate(psVector)
  private val currM2: PSVector = PSVector.duplicate(psVector)
  private val currL1: PSVector = PSVector.duplicate(psVector)
  private var totalCnt: Long = 0
  private var totalWeightSum: Double = 0.0
  private var weightSquareSum: Double = 0.0
  private val weightSum: PSVector = PSVector.duplicate(psVector)
  private val nnz: PSVector = PSVector.duplicate(psVector)
  private val currMax: PSVector = PSVector.duplicate(psVector)
  private val currMin: PSVector = PSVector.duplicate(psVector)

  /**
   * Add a new sample to this summarizer, and update the statistical summary.
   *
   * @param sample The sample in dense/sparse vector format to be added into this summarizer.
   * @return This MultivariateOnlineSummarizer object.
   */
  @Since("1.1.0")
  def add(sample: Vector): this.type = add(sample, 1.0)

  private[spark] def add(instance: Vector, weight: Double): this.type = {
    require(weight >= 0.0, s"sample weight, ${weight} has to be >= 0.0")
    if (weight == 0.0) return this

    require(n == instance.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${instance.size}.")

    val prevMean = currMean.pull()
    val prevWeight = weightSum.pull()

    val deltaMean = Array.ofDim[Double](n)
    val deltaM2n = Array.ofDim[Double](n)
    val deltaM2 = Array.ofDim[Double](n)
    val deltaL1 = Array.ofDim[Double](n)
    val deltaWeightSum = Array.ofDim[Double](n)
    val deltaNumNonzeros = Array.ofDim[Double](n)

    instance.foreachActive { (index, value) =>
      if (value != 0.0) {
        val diff = value - prevMean(index)
        deltaMean(index) = weight * diff / (prevWeight(index) + weight)
        val localMean = prevMean(index) + deltaMean(index)
        deltaM2n(index) = weight * (value - localMean) * diff
        deltaM2(index) = weight * value * value
        deltaL1(index) = weight * math.abs(value)

        deltaWeightSum(index) = weight
        deltaNumNonzeros(index) = 1
      }
    }

    currMean.toCache.incrementWithCache(deltaMean)
    currM2n.toCache.incrementWithCache(deltaM2n)
    currM2.toCache.incrementWithCache(deltaM2)
    currL1.toCache.incrementWithCache(deltaL1)
    weightSum.toCache.incrementWithCache(deltaWeightSum)
    nnz.toCache.incrementWithCache(deltaNumNonzeros)
    currMax.toCache.mergeMax(instance.toArray)
    currMin.toCache.mergeMin(instance.toArray)

    totalWeightSum += weight
    weightSquareSum += weight * weight
    totalCnt += 1
    this
  }

  /**
   * Merge another MultivariateOnlineSummarizer, and update the statistical summary.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other MultivariateOnlineSummarizer to be merged.
   * @return This MultivariateOnlineSummarizer object.
   */
  @Since("1.1.0")
  def merge(other: PSMultivariateOnlineSummarizer): this.type = {
    if (this.totalWeightSum != 0.0 && other.totalWeightSum != 0.0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      totalWeightSum += other.totalWeightSum
      weightSquareSum += other.weightSquareSum
    }

    this
  }

  /**
   * access n.
   */
  def dimension: Int = n

  /**
   * Sample mean of each dimension.
   *
   */
  @Since("1.1.0")
  def mean: PSVector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")
    val brzMean  = currMean.toBreeze :* (weightSum.toBreeze :/ totalWeightSum)
    brzMean.component
  }

  /**
   * Unbiased estimate of sample variance of each dimension.
   *
   */
  @Since("1.1.0")
  def variance: PSVector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var brzVariance: BreezePSVector = null

    val denominator = totalWeightSum - (weightSquareSum / totalWeightSum)

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      brzVariance = (currM2n.toBreeze :* totalWeightSum) :- (currMean.toBreeze :*
        currMean.toBreeze :* weightSum.toBreeze :* (weightSum.toBreeze :- totalWeightSum))
      brzVariance :/= (totalWeightSum * denominator)
    }

    brzVariance.component
  }

  def std: PSVector = {
    BreezePSVector.math.sqrt(this.variance.toBreeze).component
  }

  /**
   * Sample size.
   *
   */
  @Since("1.1.0")
  def count: Long = totalCnt

  /**
   * total weigth sum.
   *
   */
  def totalWeight: Double = totalWeightSum

  /**
   * Number of nonzero elements in each dimension.
   *
   */
  @Since("1.1.0")
  def numNonzeros: PSVector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")
    nnz
  }

  /**
   * Maximum value of each dimension.
   *
   */
  @Since("1.1.0")
  def max: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val localNnz = nnz.pull()
    val localCurrMax = currMax.pull()

    var i = 0
    while (i < n) {
      if ((localNnz(i) < totalCnt) && (localCurrMax(i) < 0.0)) localCurrMax(i) = 0.0
      i += 1
    }
    Vectors.dense(localCurrMax)
  }

  /**
   * Minimum value of each dimension.
   *
   */
  @Since("1.1.0")
  def min: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val localNnz = nnz.pull()
    val localCurrMin = currMin.pull()

    var i = 0
    while (i < n) {
      if ((localNnz(i) < totalCnt) && (localCurrMin(i) > 0.0)) localCurrMin(i) = 0.0
      i += 1
    }
    Vectors.dense(localCurrMin)
  }

  /**
   * L2 (Euclidian) norm of each dimension.
   *
   */
  @Since("1.2.0")
  def normL2: PSVector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")
    BreezePSVector.math.sqrt(currM2.toBreeze).component
  }

  /**
   * L1 norm of each dimension.
   *
   */
  @Since("1.2.0")
  def normL1: PSVector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")
    currL1
  }
}
