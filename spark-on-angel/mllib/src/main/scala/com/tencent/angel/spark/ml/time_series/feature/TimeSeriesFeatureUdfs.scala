package com.tencent.angel.spark.ml.time_series.feature

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

trait TimeSeriesFeatureUdfs {

  private[time_series] def isNullable(v: Vector, len: Int): Boolean = {
    v.size < len
  }

  private[time_series] val getMaximum = (v: Vector) => {
    if (isNullable(v, 1)) Double.NaN else v(v.argmax)
  }

  private[time_series] val getMinimum = (v: Vector) => {
    if (isNullable(v, 1)) Double.NaN else v.toArray.min
  }

  private[time_series] val getMean = (v: Vector) => {
    if (isNullable(v, 1)) {
      Double.NaN
    }
    else {
      var sum = 0.0
      for (i <- v.toArray) {
        sum += i
      }
      sum / v.size
    }
  }

  private[time_series] val getVariance = (v: Vector) => {
    if (isNullable(v, 1)) {
      Double.NaN
    }
    else if (isNullable(v, 2)) {
      0.0
    }
    else {
      val mean = getMean(v)
      var sum = 0.0
      for (i <- v.toArray) {
        sum += math.pow(i - mean, 2)
      }
      sum / (v.size - 1)
    }
  }

  private[time_series] val getStandardVariance = (v: Vector) => {
    math.sqrt(getVariance(v))
  }

  // pandas.Series.skew
  private[time_series] val getSkewness = (v: Vector) => {
    if (isNullable(v, 3) || getVariance(v) == 0) {
      Double.NaN
    } else {
      val mean = getMean(v)
      val std = getStandardVariance(v)
      val n = v.size.toDouble

      var sum = 0.0
      for (i <- v.toArray) {
        sum += math.pow(i - mean, 3) / n
      }

      sum / math.pow(std, 3) * math.pow(n, 2) / (n - 1) / (n - 2)
    }
  }

  // pandas.Series.kurt
  private[time_series] val getKurtosis = (v: Vector) => {
    if (isNullable(v, 4) || getVariance(v) == 0) {
      Double.NaN
    } else {
      val n = v.size.toDouble
      val coefficient = n * (n + 1) / (n - 1) / (n - 2) / (n - 3)
      val bias = 3 * math.pow(n - 1, 2) / (n - 2) / (n - 3)
      val mean = getMean(v)
      val std = getStandardVariance(v)

      var sum = 0.0
      for (i <- v.toArray) {
        sum += Math.pow((i - mean) / std, 4)
      }
      coefficient * sum - bias
    }
  }

  private[time_series] val getMedian = (v: Vector) => {
    if (isNullable(v, 1)) {
      Double.NaN
    }
    else {
      val sortedArray = v.toArray.sorted
      val N = v.size
      if (N % 2 == 0) {
        (sortedArray(N / 2 - 1) + sortedArray(N / 2)) / 2
      }
      else {
        sortedArray(N / 2)
      }
    }
  }

  private[time_series] val getRange = (v: Vector) => {
    getMaximum(v) - getMinimum(v)
  }

  private[time_series] val getSumOfSquare = (v: Vector) => {
    if (isNullable(v, 1)) {
      Double.NaN
    } else {
      var sum = 0.0
      for (i <- v.toArray) {
        sum += math.pow(i, 2)
      }
      sum
    }
  }

  private[time_series] val getMeanChange = (v: Vector) => {
    if (isNullable(v, 2)) Double.NaN else (v(v.size - 1) - v(0)) / (v.size - 1)
  }

  // Above mean: value > mean
  private[time_series] val getCountAboveMean = (v: Vector) => {
    if (isNullable(v, 1)) {
      0
    } else {
      var count = 0
      val mean = getMean(v)
      for (i <- v.toArray) {
        if (i > mean) {
          count += 1
        }
      }
      count
    }
  }

  // Below mean: value < mean
  private[time_series] val getCountBelowMean = (v: Vector) => {
    if (isNullable(v, 1)) {
      0
    } else {
      var count = 0
      val mean = getMean(v)
      for (i <- v.toArray) {
        if (i < mean) {
          count += 1
        }
      }
      count
    }
  }

  private[time_series] val getFirstLocationOfMaximum = (v: Vector) => {
    if (isNullable(v, 1)) Double.NaN else v.argmax.toDouble / v.size
  }

  private[time_series] val getLastLocationOfMaximum = (v: Vector) => {
    1 - getFirstLocationOfMaximum(Vectors.dense(v.toArray.reverse))
  }

  private[time_series] val getFirstLocationOfMinimum = (v: Vector) => {
    if (isNullable(v, 1)) {
      Double.NaN
    } else {
      var minimumLocation = 0.0
      var currMin = Double.MaxValue
      for (i <- 0 until v.size) {
        if (v(i) < currMin) {
          minimumLocation = i
          currMin = v(i)
        }
      }
      minimumLocation / v.size
    }
  }

  private[time_series] val getLastLocationOfMinimum = (v: Vector) => {
    1 - getFirstLocationOfMinimum(Vectors.dense(v.toArray.reverse))
  }

  private[time_series] val isHasDuplicate = (v: Vector) => {
    if (isNullable(v, 1)) 0 else if (v.toArray.distinct.length == v.size) 0 else 1
  }

  private[time_series] val isHasDuplicateMaximum = (v: Vector) => {
    if (isNullable(v, 1)) {
      0
    } else {
      val maximum = getMaximum(v)
      var maxCount = 0
      for (i <- v.toArray) {
        if (i == maximum) {
          maxCount += 1
        }
      }
      if (maxCount > 1) 1 else 0
    }
  }

  private[time_series] val isHasDuplicateMinimum = (v: Vector) => {
    if (isNullable(v, 1)) {
      0
    } else {
      val minimum = getMinimum(v)
      var minCount = 0
      for (i <- v.toArray) {
        if (i == minimum) {
          minCount += 1
        }
      }

      if (minCount > 1) 1 else 0
    }
  }

  private[time_series] val getLongestStrikeAboveMean = (v: Vector) => {
    val mean = getMean(v)
    var currentStrikeLength = 0
    var maxStrikeLength = 0
    for (i <- v.toArray) {
      if (i >= mean) {
        currentStrikeLength += 1
        if (currentStrikeLength > maxStrikeLength) {
          maxStrikeLength = currentStrikeLength
        }
      }
      else {
        currentStrikeLength = 0
      }
    }
    maxStrikeLength
  }

  private[time_series] val getLongestStrikeBelowMean = (v: Vector) => {
    val mean = getMean(v)
    var currentStrikeLength = 0
    var maxStrikeLength = 0
    for (i <- v.toArray) {
      if (i <= mean) {
        currentStrikeLength += 1
        if (currentStrikeLength > maxStrikeLength) {
          maxStrikeLength = currentStrikeLength
        }
      }
      else {
        currentStrikeLength = 0
      }
    }
    maxStrikeLength
  }

  private[time_series] val getRatioOfReoccurringValuesToAllValues = (v: Vector) => {
    if (isNullable(v, 1)) {
      Double.NaN
    } else {
      val counts = v.toArray.groupBy(identity).mapValues(_.length)
      var result = 0.0
      for (i <- counts.values) {
        if (i > 1) result += i
      }
      result / v.size
    }
  }

  private[time_series] val getBinnedEntropy = (v: Vector, numBins: Int) => {
    if (v.size < 1 || numBins < 1) {
      Double.NaN
    } else {
      val arr = v.toArray
      val (max, min) = (arr.max, arr.min)
      val width = (max - min) / numBins
      val edges = (1 to numBins).map(x => min + width * x)
      val counts = new Array[Double](numBins)

      // count the number of elements in each bins.
      for (i <- arr) {
        var j = 0
        var matched = false
        while (j < numBins && !matched) {
          if (i < edges(j)) {
            counts(j) += 1
            matched = true
          }
          j += 1
        }
        if (!matched) {
          counts(numBins - 1) += 1
        }
      }

      val probs = counts.map(x => x / v.size)
      var entropy = 0.0

      for (prob <- probs) {
        if (prob > 0) {
          entropy += prob * math.log(prob) * -1
        }
      }

      entropy
    }
  }

  private[time_series] val getC3 = (v: Vector, lag: Int) => {
    if (2 * lag >= v.size) {
      0
    } else {
      val arr = v.toArray
      var c3 = 0.0
      for (i <- 0 until v.size - 2 * lag) {
        c3 += (arr(i + 2 * lag) * arr(i + lag) * arr(i))
      }
      c3 / (v.size - 2 * lag)
    }
  }

  val maximum: UserDefinedFunction = udf(getMaximum)

  val minimum: UserDefinedFunction = udf(getMinimum)

  val mean: UserDefinedFunction = udf(getMean)

  val variance: UserDefinedFunction = udf(getVariance)

  val std: UserDefinedFunction = udf(getStandardVariance)

  val skewness: UserDefinedFunction = udf(getSkewness)

  val kurtosis: UserDefinedFunction = udf(getKurtosis)

  val median: UserDefinedFunction = udf(getMedian)

  val range: UserDefinedFunction = udf(getRange)

  val sumOfSquare: UserDefinedFunction = udf(getSumOfSquare)

  val meanChange: UserDefinedFunction = udf(getMeanChange)

  val countAboveMean: UserDefinedFunction = udf(getCountAboveMean)

  val countBelowMean: UserDefinedFunction = udf(getCountBelowMean)

  val firstLocationOfMaximum: UserDefinedFunction = udf(getFirstLocationOfMaximum)

  val lastLocationOfMaximum: UserDefinedFunction = udf(getLastLocationOfMaximum)

  val firstLocationOfMinimum: UserDefinedFunction = udf(getFirstLocationOfMinimum)

  val lastLocationOfMinimum: UserDefinedFunction = udf(getLastLocationOfMinimum)

  val hasDuplicate: UserDefinedFunction = udf(isHasDuplicate)

  val hasDuplicateMaximum: UserDefinedFunction = udf(isHasDuplicateMaximum)

  val hasDuplicateMinimum: UserDefinedFunction = udf(isHasDuplicateMinimum)

  val longestStrikeAboveMean: UserDefinedFunction = udf(getLongestStrikeAboveMean)

  val longestStrikeBelowMean: UserDefinedFunction = udf(getLongestStrikeBelowMean)

  val ratioOfReoccurringValuesToAllValues: UserDefinedFunction =
    udf(getRatioOfReoccurringValuesToAllValues)

  val binnedEntropy: UserDefinedFunction = udf(getBinnedEntropy)

  val c3: UserDefinedFunction = udf(getC3)
}
