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

package com.tencent.angel.serving.common

import com.tencent.angel.ml.math.TVector
import com.tencent.angel.serving.{ChunkedShardingData, ChunkedShardingDataList, ServingLocation}

/**
  * the predict split data
  * @param splitID the split ID
  * @param targets the nodes witch has loaded split
  * @param data the predict data
  * @tparam V
  */
class PredictSplitData[V <: TVector](val splitID: ModelSplitID, val targets: Array[ServingLocation], val data: ChunkedShardingData[V]) {
}


/**
  * the predict split data list
  * @param splitID the split ID
  * @param targets the nodes witch has loaded split
  * @param data the predict data list
  * @tparam V
  */
class PredictSplitDataList[V <: TVector](val splitID: ModelSplitID, val targets: Array[ServingLocation], val data: ChunkedShardingDataList[V]) {
}
