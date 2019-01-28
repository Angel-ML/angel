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


package com.tencent.angel.spark.ml.tree.data

import java.{util => ju}
import com.tencent.angel.spark.ml.tree.gbdt.histogram.Histogram
import com.tencent.angel.spark.ml.tree.gbdt.histogram.GradPair


object InstanceRow {
  def apply(indices: Array[Int], bins: Array[Int]): InstanceRow =
    new InstanceRow(indices, bins, 0, indices.length)

  def apply(allIndices: Array[Int], allBins: Array[Int],
            from: Int, until: Int): InstanceRow = new InstanceRow(allIndices, allBins, from, until)
}

class InstanceRow(private var allIndices: Array[Int], private var allBins: Array[Int],
                  private var start: Int, private var end: Int) extends Serializable {

  def get(fid: Int): Int = {
    val t = ju.Arrays.binarySearch(allIndices, start, end, fid)
    if (t >= 0) allBins(t) else -1
  }

  def accumulate(histograms: Array[Histogram], gradPair: GradPair,
                 isFeatUsed: Array[Boolean], featOffset: Int = 0): Unit = {
    for (i <- start until end) {
      if (isFeatUsed(allIndices(i) - featOffset))
        histograms(allIndices(i) - featOffset).accumulate(allBins(i), gradPair)
    }
  }

  def indices: Array[Int] = ???

  def bins: Array[Int] = ???

  def size: Int = end - start
}
