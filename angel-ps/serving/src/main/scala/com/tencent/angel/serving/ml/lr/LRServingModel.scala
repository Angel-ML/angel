/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving.ml.lr

import com.tencent.angel.ml.math.TVector
import com.tencent.angel.serving._

/**
  * LR Model
  */
class LRServingModel extends ShardingModel {

  // Get LR weight params on this PS
  // weight: LR model params
  // offset: start colId
  // dim: the length of cols on ths PS
  private val weightMat = "lr_weight"
  private var weight: TVector = _
  private var offset: Long = 0
  private var dimension: Long = 0

  override def load(): Unit = {
    weight = getRow(weightMat, 0)
    dimension = getDimension(weightMat)
    offset = getColumnOffset(weightMat)
  }

  /**
    * Predict a data with LR model
    *
    * @param data predict data
    * @return predict a data and return the predict result, for LR, y = dot(weight, x)
    */
  override def predict(data: ShardingData): PredictResult = {
    new PredictResult(data.getData(offset, dimension).dot(weight))
  }

}
