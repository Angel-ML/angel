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
import com.tencent.angel.ml.math.vector.{SparseLongKeyDummyVector, SparseLongKeySortedDoubleVector}


abstract class DataParser64 {
  def parse(value: String): LabeledData
}

case class Dummy64DataParser(val maxDim: Long, val negY: Boolean) extends DataParser {
  override def parse(value: String): LabeledData = {
    if (null == value) {
      return null
    }
    val splits = value.split(",")
    if (splits.length < 1) {
      return null
    }
    val x = new SparseLongKeyDummyVector(maxDim, splits.length - 1)
    var y = splits(0).toDouble

    // y should be +1 or -1 when classification.
    if (negY && y != 1)
      y = -1

    splits.tail.map(idx => x.set(idx.toLong, 1))
    new LabeledData(x, y)
  }
}

case class LibSVM64DataParser(val maxDim: Long, val negY: Boolean) extends DataParser {
  type V = SparseLongKeySortedDoubleVector

  override def parse(text: String): LabeledData = {
    if (null == text) {
      return null
    }
    val splits = text.trim.split("\\s+")

    if (splits.length < 1)
      return null

    val len: Int = splits.length - 1
    val keys: Array[Long] = new Array[Long](len)
    val vals: Array[Double] = new Array[Double](len)
    var y: Double = splits(0).toDouble

    // y should be +1 or -1 when classification.
    if (negY && y != 1)
      y = -1

    var kv = Array[String]()
    var key: Long = -1
    var value: Double = -1.0
    var i: Int = 0
    while (i < len) {
      kv = splits(i + 1).trim.split(":")
      key = kv(0).toLong
      value = kv(1).toDouble
      keys(i) = key
      vals(i) = value
      i += 1
    }

    val x = new SparseLongKeySortedDoubleVector(maxDim, keys, vals)

    new LabeledData(x, y)
  }
}

object DataParser64 {

  def apply(dataFormat: String, maxDim: Long, negY: Boolean): DataParser = {
    dataFormat match {
      case "dummy" => new Dummy64DataParser(maxDim, negY)
      case "libsvm" => new LibSVM64DataParser(maxDim, negY)
    }
  }
}

