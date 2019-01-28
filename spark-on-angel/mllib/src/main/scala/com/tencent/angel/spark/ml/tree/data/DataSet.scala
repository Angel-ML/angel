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

private case class Partition(indices: Array[Int], bins: Array[Int], indexEnd: Array[Int])

class DataSet(numPartition: Int, numInstance: Int) {

  private val partitions = new Array[Partition](numPartition)
  private val partOffsets = new Array[Int](numPartition)
  private val insLayouts = new Array[Int](numInstance)

  def setPartition(partId: Int, indices: Array[Int],
                   values: Array[Int], indexEnd: Array[Int]): Unit = {
    partitions(partId) = Partition(indices, values, indexEnd)
    if (partId == 0) {
      partOffsets(partId) = 0
    } else {
      partOffsets(partId) = partOffsets(partId - 1) +
        partitions(partId - 1).indexEnd.length
    }
    for (partInsId <- indexEnd.indices)
      insLayouts(partOffsets(partId) + partInsId) = partId
  }

  def get(insId: Int, fid: Int): Int = {
    val partId = insLayouts(insId)
    val partition = partitions(partId)
    val partInsId = insId - partOffsets(partId)
    val start = if (partInsId == 0) 0 else partition.indexEnd(partInsId - 1)
    val end = partition.indexEnd(partInsId)
    val t = ju.Arrays.binarySearch(partition.indices, start, end, fid)
    if (t >= 0) partition.bins(t) else -1
  }

  def accumulate(histograms: Array[Histogram], insId: Int, gradPair: GradPair,
                 isFeatUsed: Array[Boolean], featOffset: Int = 0): Unit = {
    val partId = insLayouts(insId)
    val indices = partitions(partId).indices
    val bins = partitions(partId).bins
    val partInsId = insId - partOffsets(partId)
    val start = if (partInsId == 0) 0 else partitions(partId).indexEnd(partInsId - 1)
    val end = partitions(partId).indexEnd(partInsId)
    for (i <- start until end) {
      if (isFeatUsed(indices(i) - featOffset))
        histograms(indices(i) - featOffset).accumulate(bins(i), gradPair)
    }
  }

}
