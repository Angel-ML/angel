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

/**
 *
 * This class is a copy of MultivariateOnlineSummarizer.scala in org.apache.spark.mllib.stat
 * package of spark 2.1.0 MLlib.
 *
 * Based on the original version, we improve the algorithm with Angel PS-Service.
 */

package org.apache.spark.mllib.stat

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.ml.linalg.ElementwiseSlicing
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * :: DeveloperApi ::
 * PSMultivariateOnlineSummarizer implements MultivariateStatisticalSummary to compute the mean,
 * variance, minimum, maximum, counts, and nonzero counts for instances in sparse or dense vector
 * format in an online fashion.
 *
 * Two PSMultivariateOnlineSummarizer can be merged together to have a statistical summary of
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
class PSMultivariateOnlineSummarizer extends MultivariateStatisticalSummary with Serializable {

  private var n = 0
  private var currMean: Array[Float] = _
  private var currM2n: Array[Float] = _
  private var currM2: Array[Float] = _
  private var currL1: Array[Float] = _
  private var totalCnt: Long = 0
  var totalWeightSum: Float = 0.0f
  private var weightSquareSum: Float = 0.0f
  private var weightSum: Array[Float] = _
  private var nnz: Array[Long] = _
  private var currMax: Array[Float] = _
  private var currMin: Array[Float] = _

  /**
   * Add a new sample to this summarizer, and update the statistical summary.
   *
   * @param sample The sample in dense/sparse vector format to be added into this summarizer.
   * @return This PSMultivariateOnlineSummarizer object.
   */
  @Since("1.1.0")
  def add(sample: Vector): this.type = add(sample, 1.0)

  private[spark] def add(instance: Vector, dWeight: Double): this.type = {
    val weight = dWeight.toFloat
    require(weight >= 0.0, s"sample weight, ${weight} has to be >= 0.0")
    if (weight == 0.0f) return this

    if (n == 0) {
      require(instance.size > 0, s"Vector should have dimension larger than zero.")
      n = instance.size

      currMean = Array.ofDim[Float](n)
      currM2n = Array.ofDim[Float](n)
      currM2 = Array.ofDim[Float](n)
      currL1 = Array.ofDim[Float](n)
      weightSum = Array.ofDim[Float](n)
      nnz = Array.ofDim[Long](n)
      currMax = Array.fill[Float](n)(Float.MinValue)
      currMin = Array.fill[Float](n)(Float.MaxValue)
    }

    require(n == instance.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${instance.size}.")

    val localCurrMean = currMean
    val localCurrM2n = currM2n
    val localCurrM2 = currM2
    val localCurrL1 = currL1
    val localWeightSum = weightSum
    val localNumNonzeros = nnz
    val localCurrMax = currMax
    val localCurrMin = currMin
    instance.foreachActive { (index, value) =>
      if (value != 0.0) {
        if (localCurrMax(index) < value) {
          localCurrMax(index) = value.toFloat
        }
        if (localCurrMin(index) > value) {
          localCurrMin(index) = value.toFloat
        }

        val prevMean = localCurrMean(index)
        val diff = value.toFloat - prevMean
        localCurrMean(index) = (prevMean + weight * diff / (localWeightSum(index) + weight)).toFloat
        localCurrM2n(index) += weight * (value.toFloat - localCurrMean(index)) * diff
        localCurrM2(index) += weight * value.toFloat * value.toFloat
        localCurrL1(index) += weight * math.abs(value.toFloat)

        localWeightSum(index) += weight
        localNumNonzeros(index) += 1
      }
    }

    totalWeightSum += weight
    weightSquareSum += weight * weight
    totalCnt += 1
    this
  }

  /**
   * Merge another PSMultivariateOnlineSummarizer, and update the statistical summary.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other PSMultivariateOnlineSummarizer to be merged.
   * @return This PSMultivariateOnlineSummarizer object.
   */
  @Since("1.1.0")
  def merge(other: PSMultivariateOnlineSummarizer): this.type = {
    if (this.totalWeightSum != 0.0 && other.totalWeightSum != 0.0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      totalWeightSum += other.totalWeightSum
      weightSquareSum += other.weightSquareSum
      var i = 0
      while (i < n) {
        val thisNnz = weightSum(i)
        val otherNnz = other.weightSum(i)
        val totalNnz = thisNnz + otherNnz
        val totalCnnz = nnz(i) + other.nnz(i)
        if (totalNnz != 0.0) {
          val deltaMean = other.currMean(i) - currMean(i)
          // merge mean together
          currMean(i) += deltaMean * otherNnz / totalNnz
          // merge m2n together
          currM2n(i) += other.currM2n(i) + deltaMean * deltaMean * thisNnz * otherNnz / totalNnz
          // merge m2 together
          currM2(i) += other.currM2(i)
          // merge l1 together
          currL1(i) += other.currL1(i)
          // merge max and min
          currMax(i) = math.max(currMax(i), other.currMax(i))
          currMin(i) = math.min(currMin(i), other.currMin(i))
        }
        weightSum(i) = totalNnz
        nnz(i) = totalCnnz
        i += 1
      }
    } else if (totalWeightSum == 0.0 && other.totalWeightSum != 0.0) {
      this.n = other.n
      this.currMean = other.currMean.clone()
      this.currM2n = other.currM2n.clone()
      this.currM2 = other.currM2.clone()
      this.currL1 = other.currL1.clone()
      this.totalCnt = other.totalCnt
      this.totalWeightSum = other.totalWeightSum
      this.weightSquareSum = other.weightSquareSum
      this.weightSum = other.weightSum.clone()
      this.nnz = other.nnz.clone()
      this.currMax = other.currMax.clone()
      this.currMin = other.currMin.clone()
    }
    this
  }

  /**
   * Sample mean of each dimension.
   *
   */
  @Since("1.1.0")
  override def mean: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMean = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      realMean(i) = currMean(i) * (weightSum(i) / totalWeightSum)
      i += 1
    }
    Vectors.dense(realMean)
  }

  /**
   * Unbiased estimate of sample variance of each dimension.
   *
   */
  @Since("1.1.0")
  override def variance: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realVariance = Array.ofDim[Double](n)

    val denominator = totalWeightSum - (weightSquareSum / totalWeightSum)

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean
      var i = 0
      val len = currM2n.length
      while (i < len) {
        realVariance(i) = (currM2n(i) + deltaMean(i) * deltaMean(i) * weightSum(i) *
          (totalWeightSum - weightSum(i)) / totalWeightSum) / denominator
        i += 1
      }
    }
    Vectors.dense(realVariance)
  }

  def std: Vector = {
    Vectors.dense(variance.toArray.map(x => math.sqrt(x)))
  }

  /**
   * Sample size.
   *
   */
  @Since("1.1.0")
  override def count: Long = totalCnt

  /**
   * Number of nonzero elements in each dimension.
   *
   */
  @Since("1.1.0")
  override def numNonzeros: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(nnz.map(_.toDouble))
  }

  /**
   * Maximum value of each dimension.
   *
   */
  @Since("1.1.0")
  override def max: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0f
      i += 1
    }
    Vectors.dense(currMax.map(_.toDouble))
  }

  /**
   * Minimum value of each dimension.
   *
   */
  @Since("1.1.0")
  override def min: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0f
      i += 1
    }
    Vectors.dense(currMin.map(_.toDouble))
  }

  /**
   * L2 (Euclidian) norm of each dimension.
   *
   */
  @Since("1.2.0")
  override def normL2: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMagnitude = Array.ofDim[Double](n)

    var i = 0
    val len = currM2.length
    while (i < len) {
      realMagnitude(i) = math.sqrt(currM2(i))
      i += 1
    }
    Vectors.dense(realMagnitude)
  }

  /**
   * L1 norm of each dimension.
   *
   */
  @Since("1.2.0")
  override def normL1: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(currL1.map(_.toDouble))
  }
}

object PSMultivariateOnlineSummarizer {
  implicit object SummarizerSlicing extends ElementwiseSlicing[PSMultivariateOnlineSummarizer] {
    override def slice(x: PSMultivariateOnlineSummarizer, num: Int):
        Iterator[PSMultivariateOnlineSummarizer] = {
      val sliceLength = math.ceil(x.n.toDouble / num).toInt
      val meanIter = x.currMean.sliding(sliceLength, sliceLength)
      val m2nIter = x.currM2n.sliding(sliceLength, sliceLength)
      val m2Iter = x.currM2.sliding(sliceLength, sliceLength)
      val l1Iter = x.currL1.sliding(sliceLength, sliceLength)
      val weightSumIter = x.weightSum.sliding(sliceLength, sliceLength)
      val nnzIter = x.nnz.sliding(sliceLength, sliceLength)
      val maxIter = x.currMax.sliding(sliceLength, sliceLength)
      val minIter = x.currMin.sliding(sliceLength, sliceLength)

      meanIter.map { mean =>
        val summary = new PSMultivariateOnlineSummarizer
        summary.currMean = mean
        summary.currM2n = m2nIter.next()
        summary.currM2 = m2Iter.next()
        summary.currL1 = l1Iter.next()
        summary.totalCnt = x.totalCnt
        summary.totalWeightSum = x.totalWeightSum
        summary.weightSquareSum = x.weightSquareSum
        summary.weightSum = weightSumIter.next()
        summary.nnz = nnzIter.next()
        summary.currMax = maxIter.next()
        summary.currMin = minIter.next()
        summary.n = summary.currMean.length
        summary
      }
    }

    override def compose(iter: Iterator[PSMultivariateOnlineSummarizer]):
        PSMultivariateOnlineSummarizer = {
      val (iter1, iter2) = iter.duplicate

      // what if iter1 is empty
      val length = iter1.map(_.n).sum

      val compSummary = new PSMultivariateOnlineSummarizer
      compSummary.n = length
      compSummary.currMean = new Array[Float](length)
      compSummary.currM2n = new Array[Float](length)
      compSummary.currM2 = new Array[Float](length)
      compSummary.currL1 = new Array[Float](length)
      compSummary.weightSum = new Array[Float](length)
      compSummary.nnz = new Array[Long](length)
      compSummary.currMax = new Array[Float](length)
      compSummary.currMin = new Array[Float](length)

      var accumNum = 0
      iter2.foreach { summary =>
        summary.currMean.copyToArray(compSummary.currMean, accumNum, summary.currMean.length)
        summary.currM2n.copyToArray(compSummary.currM2n, accumNum, summary.currM2n.length)
        summary.currM2.copyToArray(compSummary.currM2, accumNum, summary.currM2.length)
        summary.currL1.copyToArray(compSummary.currL1, accumNum, summary.currL1.length)
        compSummary.totalCnt = summary.totalCnt
        compSummary.totalWeightSum = summary.totalWeightSum
        compSummary.weightSquareSum = summary.weightSquareSum
        summary.weightSum.copyToArray(compSummary.weightSum, accumNum, summary.weightSum.length)
        summary.nnz.copyToArray(compSummary.nnz, accumNum, summary.nnz.length)
        summary.currMax.copyToArray(compSummary.currMax, accumNum, summary.currMax.length)
        summary.currMin.copyToArray(compSummary.currMin, accumNum, summary.currMin.length)
        accumNum += summary.currMean.length
      }
      compSummary
    }
  }
}
