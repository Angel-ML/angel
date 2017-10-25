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
package com.tencent.angel.ml.utils

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{SparseDummyVector, SparseDoubleSortedVector}


abstract class DataParser{
  def parse(value: String): LabeledData
}

case class DummyDataParser(val maxDim: Int, val negY: Boolean) extends DataParser{
  override def parse(value: String): LabeledData = {
    if (null == value) {
      return null
    }
    val splits = value.split(",")
    if (splits.length < 1) {
      return null
    }
    val x = new SparseDummyVector(maxDim, splits.length - 1)
    var y = splits(0).toDouble

    // y should be +1 or -1 when classification.
    if (negY && y != 1)
      y = -1

    splits.tail.map(idx => x.set(idx.toInt, 1))
    new LabeledData(x, y)
  }
}

case class LibSVMDataParser(val maxDim: Int, val negY: Boolean) extends DataParser {
  type V = SparseDoubleSortedVector

  override def parse(text: String): LabeledData = {
    if (null == text) {
      return null
    }
    val splits = text.trim.split("\\s+")

    if (splits.length < 1)
      return null

    val len: Int = splits.length - 1
    val keys: Array[Int] = new Array[Int](len)
    val vals: Array[Double] = new Array[Double](len)
    var y: Double = splits(0).toDouble

    // y should be +1 or -1 when classification.
    if (negY && y != 1)
      y = -1

    var kv = Array[String]()
    var key: Int = -1
    var value: Double = -1.0
    var i: Int = 0
    while (i < len) {
      kv = splits(i + 1).trim.split(":")
      key = kv(0).toInt
      value = kv(1).toDouble
      keys(i) = key
      vals(i) = value
      i += 1
    }

    val x = new SparseDoubleSortedVector(maxDim, keys, vals)

    new LabeledData(x, y)
  }
}

object DataParser {

  def apply(dataFormat: String, maxDim: Int, negY: Boolean) :DataParser = {
    dataFormat match {
      case "dummy" => new DummyDataParser(maxDim, negY)
      case "libsvm" => new LibSVMDataParser(maxDim, negY)
    }


  }
}

