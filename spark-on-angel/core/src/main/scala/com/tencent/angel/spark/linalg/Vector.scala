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
 */

package com.tencent.angel.spark.linalg

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap

trait Vector extends Serializable {
  def length: Long
  def apply(index: Long): Double

  def toDense: DenseVector

  def toSparse: SparseVector
}

class DenseVector(val values: Array[Double]) extends Vector {
  def length: Long = values.length

  def apply(index: Long): Double = values(index.toInt)

  def toDense: DenseVector = this

  def toSparse: SparseVector = {
    val nzIndices = new Array[Long](nnz)
    val nzValues = new Array[Double](nnz)
    var i = 0
    values.indices.foreach { j =>
      if (values(j) != 0.0) {
        nzIndices(i) = j.toLong
        nzValues(i) = values(j)
        i += 1
      }
    }
    new SparseVector(length, nzIndices, nzValues)
  }

  def toArray: Array[Double] = values

  def nnz: Int = {
    var nonZeroNum = 0
    values.foreach { x =>
      if (x != 0) nonZeroNum += 1
    }
    nonZeroNum
  }
}

class SparseVector(override val length: Long) extends Vector {
  var keyValues: Long2DoubleOpenHashMap = new Long2DoubleOpenHashMap()

  def this(len: Long, indices: Array[Long], values: Array[Double]) {
    this(len)
    keyValues = null
    keyValues = new Long2DoubleOpenHashMap(indices, values)
  }

  def this(len: Long, pairs: Array[(Long, Double)]) {
    this(len)
    val (keys, values) = pairs.unzip
    keyValues = null
    keyValues = new Long2DoubleOpenHashMap(keys, values)
  }

  def this(len: Long, other: Long2DoubleOpenHashMap) {
    this(len)
    keyValues = null
    keyValues = other.clone()
  }

  def this(len: Long, expectedSize: Int) {
    this(len)
    keyValues = null
    keyValues = new Long2DoubleOpenHashMap(expectedSize)
  }

  def reSize(size: Long): Unit = {
    val temp = new Long2DoubleOpenHashMap(size.toInt)
    temp.putAll(keyValues)
    keyValues = temp
  }

  def toDense: DenseVector = throw new Exception("Can not convert SparseVector to DenseVector")

  def toSparse: SparseVector = this

  def nnz: Long = {
    import scala.collection.JavaConverters._
    var num = 0L
    for (entry <- keyValues.asScala) {
      if (math.abs(entry._2 - keyValues.defaultReturnValue()) > 1e-11) {
        num += 1
      }
    }
    num
  }

  def apply(index: Long): Double = {
    keyValues.get(index)
  }

  def indices: Array[Long] = {
    keyValues.keySet().toLongArray
  }

  def values: Array[Double] = {
    keyValues.values().toDoubleArray
  }

  def pairs: (Long, Array[Long], Array[Double]) = {
    require(keyValues.defaultReturnValue() == 0.0, "SparseVector default value must be 0.0")
    val keys = new Array[Long](keyValues.size())
    val values = new Array[Double](keyValues.size())
    import scala.collection.JavaConverters._
    var i: Int = 0
    for (entry <- keyValues.asScala) {
      keys(i) = entry._1
      values(i) = entry._2
      i += 1
    }
    (length, keys, values)
  }

  def put(key: Long, value: Double): Unit = {
    keyValues.put(key, value)
  }
}

class OneHotVector(override val length: Long) extends Vector {
  var indices: Array[Long] = _

  def this(length: Long, ids: Array[Long]) {
    this(length)
    require(ids.length <= length)
    indices = ids.sorted
  }

  def this(length: Long, indices: Array[Int]) {
    this(length, indices.map(_.toLong))
  }

  def apply(index: Long): Double = {
    if (java.util.Arrays.binarySearch(indices, index) >= 0) 1.0 else 0.0
  }

  def toDense: DenseVector = throw new Exception("Can not convert OneHotVector to DenseVector")

  def toSparse: SparseVector = {
    val values = Array.fill(indices.length)(1.0)
    new SparseVector(length, indices, values)
  }
}
