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
 *
 */

package com.tencent.angel.ml.matrixfactorization.threads

import java.text.DecimalFormat

import com.tencent.angel.ml.math.vector.DenseFloatVector

class UserVec(var userId: Int, var itemIds: Array[Int], var ratings: Array[Int]) {
  var rank: Int = _
  var features: DenseFloatVector = _
  val format = new DecimalFormat(".00000")

  def getUserId: Int = {
    return userId
  }

  def setRank(rank: Int) {
    this.rank = rank
  }

  def setFeatures(features: DenseFloatVector): Unit = {
    this.features = features
  }

  def initFeatures() {
    if (features == null) {
      features = new DenseFloatVector(rank)
    }

    for (i <- 0 until rank) {
      features.set(i, Math.random.toFloat)
    }

  }

  def getFeatures: DenseFloatVector = {
    return features
  }

  def getItemIds: Array[Int] = {
    return itemIds
  }

  def getRatings: Array[Int] = {
    return ratings
  }

  override def toString: String = {
    var form = new DecimalFormat("0.00000")

    var ustring = userId.toString + " "
    for (i <- 0 until rank)
      ustring = ustring + form.format(features.get(i)) + " "

    ustring
  }
}
