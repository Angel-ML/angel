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

import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.serving.ChunkedShardingData
import com.tencent.angel.serving.common.ModelSplitID
import com.tencent.angel.serving.transport.serving.ServingType.ServingType
import io.netty.buffer.ByteBuf

/**
  * single predict request message
  *
  * @param id   the split ID
  * @param data the sharding data
  */
class PredictReqMsg(val id: ModelSplitID, data: ChunkedShardingData[_]) extends ServingSplitReqMsg[ChunkedShardingData[_]] {


  override def msgType: ServingType = ServingType.PREDICT_REQUEST

  /** Number of bytes of the encoded form of this object. */
  override def encodedLength: Int = data.encodedLength()

  /**
    * Serializes this object by writing into the given ByteBuf.
    * This method must write exactly encodedLength() bytes.
    */
  override def encode(buf: ByteBuf): Unit = {
    data.encode(buf)
  }

  override def body: ChunkedShardingData[_] = data
}

object PredictReqMsg {
  def decode(model: ModelSplitID, buf: ByteBuf): PredictReqMsg = {
    new PredictReqMsg(model, ChunkedShardingData.decode(RowType.valueOf(buf.readInt()), buf))
  }
}
