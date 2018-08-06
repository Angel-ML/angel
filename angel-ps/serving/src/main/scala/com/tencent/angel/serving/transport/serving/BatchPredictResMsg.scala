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

package com.tencent.angel.serving.transport.serving

import com.tencent.angel.serving.common.ModelSplitID
import com.tencent.angel.serving.transport.serving.ServingType.ServingType
import io.netty.buffer.ByteBuf

/**
  * the batch predict response message
  * @param id the split ID
  * @param scores the scores of predict result
  */
class BatchPredictResMsg(val id: ModelSplitID, val scores: Array[Double]) extends ServingSplitResMsg[Array[Double]] {

  override def msgType: ServingType = ServingType.BATCH_PREDICT_RESPONSE

  override def encodedLength: Int = 8 * scores.length

  override def encode(buf: ByteBuf): Unit = {
    buf.writeInt(scores.length)
    scores.foreach(buf.writeDouble(_))
  }

  override def body: Array[Double] = scores

}

object BatchPredictResMsg {
  def decode(id: ModelSplitID, buf: ByteBuf): BatchPredictResMsg = {
    val len = buf.readInt
    val scores = (0 until len).map(_ => buf.readDouble())
    new BatchPredictResMsg(id, scores.toArray)
  }
}
