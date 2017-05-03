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

package org.apache.spark.mllib.stat

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import com.tencent.angel.spark.vector.BreezePSVector
import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.PSVectorPool
import com.tencent.angel.spark.PSVectorProxy

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
class PSMultivariateOnlineSummarizer(@transient private val psPool: PSVectorPool)
  extends Serializable {

  private val n = psPool.numDimensions
  private val currMean: PSVectorProxy = psPool.createZero()
  private val currM2n: PSVectorProxy = psPool.createZero()
  private val currM2: PSVectorProxy = psPool.createZero()
  private val currL1: PSVectorProxy = psPool.createZero()
  private var totalCnt: Long = 0
  private var totalWeightSum: Double = 0.0
  private var weightSquareSum: Double = 0.0
  private val weightSum: PSVectorProxy = psPool.createZero()
  private val nnz: PSVectorProxy = psPool.createZero()
  private val currMax: PSVectorProxy = psPool.createZero()
  private val currMin: PSVectorProxy = psPool.createZero()

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
    val psClient = PSClient.get

    require(n == instance.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${instance.size}.")

    val prevMean = psClient.get(currMean)
    val prevWeight = psClient.get(weightSum)

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

    psClient.increment(currMean, deltaMean)
    psClient.increment(currM2n, deltaM2n)
    psClient.increment(currM2, deltaM2)
    psClient.increment(currL1, deltaL1)
    psClient.increment(weightSum, deltaWeightSum)
    psClient.increment(nnz, deltaNumNonzeros)
    psClient.mergeMax(currMax, instance.toArray)
    psClient.mergeMin(currMin, instance.toArray)

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
  def mean: PSVectorProxy = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")
    val brzMean = currMean.mkBreeze() :* (weightSum.mkBreeze() :/ totalWeightSum)
    brzMean.proxy
  }

  /**
   * Unbiased estimate of sample variance of each dimension.
   *
   */
  @Since("1.1.0")
  def variance: PSVectorProxy = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var brzVariance: BreezePSVector = null

    val denominator = totalWeightSum - (weightSquareSum / totalWeightSum)

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      brzVariance = (currM2n.mkBreeze() :* totalWeightSum) :- (currMean.mkBreeze() :*
        currMean.mkBreeze() :* weightSum.mkBreeze() :* (weightSum.mkBreeze() :- totalWeightSum))
      brzVariance :/= (totalWeightSum * denominator)
    }

    brzVariance.proxy
  }

  def std: PSVectorProxy = {
    BreezePSVector.math.sqrt(this.variance.mkBreeze()).proxy
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
  def numNonzeros: PSVectorProxy = {
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

    val localNnz = nnz.mkLocal().get()
    val localCurrMax = currMax.mkLocal().get()

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

    val localNnz = nnz.mkLocal().get()
    val localCurrMin = currMin.mkLocal().get()

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
  def normL2: PSVectorProxy = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")
    BreezePSVector.math.sqrt(currM2.mkBreeze()).proxy
  }

  /**
   * L1 norm of each dimension.
   *
   */
  @Since("1.2.0")
  def normL1: PSVectorProxy = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")
    currL1
  }
}
