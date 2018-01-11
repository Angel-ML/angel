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

package com.tencent.angel.serving.transport.serving

import com.tencent.angel.ml.math.TVector
import com.tencent.angel.serving.ChunkedShardingDataList
import com.tencent.angel.serving.common.ModelSplitID
import com.tencent.angel.serving.transport.serving.ServingType.ServingType
import io.netty.buffer.ByteBuf

/**
  * batch predict request message
  *
  * @param id    the split ID
  * @param batch the batch data
  * @tparam V
  */
class BatchPredictReqMsg[V <: TVector](val id: ModelSplitID, batch: ChunkedShardingDataList[V]) extends ServingSplitReqMsg[ChunkedShardingDataList[V]] {

  override def msgType: ServingType = ServingType.BATCH_PREDICT_REQUEST

  override def encode(buf: ByteBuf) = {
    assert(batch != null)
    batch.encode(buf)
  }

  override def encodedLength: Int = batch.encodedLength


  override def body: ChunkedShardingDataList[V] = batch
}

object BatchPredictReqMsg {
  def apply[V <: TVector](splitID: ModelSplitID, dataList: ChunkedShardingDataList[V]): BatchPredictReqMsg[V] = {
    new BatchPredictReqMsg(splitID, dataList)
  }

  def decode[V <: TVector](id: ModelSplitID, buf: ByteBuf): BatchPredictReqMsg[V] = {
    new BatchPredictReqMsg[V](id, ChunkedShardingDataList.decode[V](buf))
  }
}

