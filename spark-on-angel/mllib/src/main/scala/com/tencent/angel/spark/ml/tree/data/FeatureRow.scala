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

import java.util

object FeatureRow {
  def compact(rows: Seq[FeatureRow]): Option[FeatureRow] = {
    val nonEmptyRows = rows.filter(!_.isEmpty)
    if (nonEmptyRows.isEmpty) {
      Option.empty
    } else {
      val size = nonEmptyRows.map(_.size).sum
      val indices = new Array[Int](size)
      val bins = new Array[Int](size)
      var offset = 0
      nonEmptyRows.sortWith((row1, row2) => row1.indices(0) < row2.indices(0))
        .foreach(row => {
          val partSize = row.size
          Array.copy(row.indices, 0, indices, offset, partSize)
          Array.copy(row.bins, 0, bins, offset, partSize)
          offset += partSize
        })
      Option(FeatureRow(indices, bins))
    }
  }
}

case class FeatureRow(indices: Array[Int], bins: Array[Int]) {
  override def toString: String = "[" + (indices, bins).zipped.map((k, v) => s"$k:$v").mkString(", ") + "]"

  def getOrElse(indice: Int, defaultBin: Int): Int = {
    val pos = util.Arrays.binarySearch(indices, indice)
    if (pos >= 0) bins(pos)
    else defaultBin
  }

  def isEmpty: Boolean = indices == null || indices.length == 0

  def size: Int = if (indices == null) 0 else indices.length
}
