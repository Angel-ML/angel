package com.tencent.angel.spark.vector

import org.apache.spark.SparkException

import com.tencent.angel.spark.{PSClient, PSVector, PSVectorProxy}

/**
 * RemotePSVector implements a set of operations between PSVector and local double array.
 */
class RemotePSVector private[spark](override val proxy: PSVectorProxy)
  extends PSVector {

  def toLocal: LocalPSVector = new LocalPSVector(proxy)

  def toBreeze: BreezePSVector = new BreezePSVector(proxy)

  import MergeType._

  @transient var mergeType: MergeType = UNDEFINED

  @transient var mergedArray: Array[Double] = null

  private def init(mergeType: MergeType): Unit = {
    if (mergedArray == null) {
      mergedArray = new Array[Double](proxy.numDimensions)
      this.mergeType = mergeType
      PSClient.get.register(this)
    } else {
      if (this.mergeType != mergeType) {
        throw new SparkException(
          "Do not use different merge methods on the same RemotePSVector!")
      }
    }
  }

  private[spark] def flush(): Unit = {
    if (mergedArray != null) {
      mergeType match {
        case INCREMENT =>
          PSClient.get.increment(proxy, mergedArray)
        case MAX =>
          PSClient.get.mergeMax(proxy, mergedArray)
        case MIN =>
          PSClient.get.mergeMin(proxy, mergedArray)
      }
      mergeType = UNDEFINED
      mergedArray = null
    }
  }

  /**
   * Increment a local dense Double array to PSVector
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def increment(delta: Array[Double]): Unit = {
    init(INCREMENT)
    require(delta.length == mergedArray.length)
    val n = mergedArray.length
    PSClient.get.BLAS.daxpy(n, 1.0, delta, 1, mergedArray, 1)
  }

  /**
   * Increment a local sparse Double array to PSVector
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def increment(indices: Array[Int], values: Array[Double]): Unit = {
    init(INCREMENT)
    require(indices.length == values.length && indices.length <= mergedArray.length)
    var i = 0
    while (i < indices.length) {
      mergedArray(indices(i)) += values(i)
      i += 1
    }
  }

  /**
   * Increment a local dense Double array to PSVector, and flush to PS nodes immediately
   */
  def incrementAndFlush(delta: Array[Double]): Unit = {
    PSClient.get.increment(proxy, delta)
  }

  /**
   * Find the maximum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMax(other: Array[Double]): Unit = {
    init(MAX)
    require(other.length == mergedArray.length)
    var i = 0
    while (i < mergedArray.length) {
      mergedArray(i) = math.max(mergedArray(i), other(i))
      i += 1
    }
  }

  /**
   * Find the maximum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMax(indices: Array[Int], values: Array[Double]): Unit = {
    init(MAX)
    require(indices.length == values.length && indices.length <= mergedArray.length)
    var i = 0
    while (i < indices.length) {
      val index = indices(i)
      mergedArray(index) = math.max(index, values(i))
      i += 1
    }
  }

  /**
   * Find the maximum number of each dimension for PSVector and local sparse array, and flush to
   * PS nodes immediately
   */
  def mergeMaxAndFlush(other: Array[Double]): Unit = {
    PSClient.get.mergeMax(proxy, other)
  }

  /**
   * Find the maximum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMin(other: Array[Double]): Unit = {
    init(MIN)
    require(other.length == mergedArray.length)
    var i = 0
    while (i < mergedArray.length) {
      mergedArray(i) = math.min(mergedArray(i), other(i))
      i += 1
    }
  }

  /**
   * Find the minimum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMin(indices: Array[Int], values: Array[Double]): Unit = {
    init(MIN)
    require(indices.length == values.length && indices.length <= mergedArray.length)
    var i = 0
    while (i < indices.length) {
      val index = indices(i)
      mergedArray(index) = math.min(index, values(i))
      i += 1
    }
  }

  /**
   * Find the minimum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMinAndFlush(other: Array[Double]): Unit = {
    PSClient.get.mergeMin(proxy, other)
  }

}

object MergeType extends Enumeration {
  type MergeType = Value
  val UNDEFINED, INCREMENT, MAX, MIN = Value
}
