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

package com.tencent.angel.spark.ml.psf.embedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitPartitionParam extends PartitionUpdateParam {

  int numPartitions;
  int maxIndex;
  int maxLength;
  int negative;
  int order;
  int partDim;
  int window;


  public InitPartitionParam(int matrixId,
                            PartitionKey partKey,
                            int numPartitions,
                            int maxIndex,
                            int maxLength,
                            int negative,
                            int order,
                            int partDim,
                            int window) {
    super(matrixId, partKey);

    this.numPartitions = numPartitions;
    this.maxIndex = maxIndex;
    this.maxLength = maxLength;
    this.negative = negative;
    this.order = order;
    this.partDim = partDim;
    this.window = window;
  }

  public InitPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);

    buf.writeInt(numPartitions);
    buf.writeInt(maxIndex);
    buf.writeInt(maxLength);
    buf.writeInt(negative);
    buf.writeInt(order);
    buf.writeInt(partDim);
    buf.writeInt(window);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    numPartitions = buf.readInt();
    maxIndex = buf.readInt();
    maxLength = buf.readInt();
    negative = buf.readInt();
    order = buf.readInt();
    partDim = buf.readInt();
    window = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 28;
  }
}
