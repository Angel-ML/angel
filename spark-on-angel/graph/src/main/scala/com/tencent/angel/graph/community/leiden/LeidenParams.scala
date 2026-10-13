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
package com.tencent.angel.graph.community.leiden

import com.tencent.angel.graph.ann.params.HasItemSep
import com.tencent.angel.graph.utils.params._


private[leiden] trait LeidenParams extends Serializable with HasWeightCol
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol
  with HasOutputCommunityIdCol with HasIsWeighted with HasPartitionNum
  with HasPSPartitionNum with HasStorageLevel with HasBatchSize
  with HasTheta with HasGamma with HasMaxIteration
  with HasMaxOptimization with HasInput with HasOutput
  with HasSrcNodeIndex with HasDstNodeIndex with HasWeightIndex
  with HasItemSep {

}
