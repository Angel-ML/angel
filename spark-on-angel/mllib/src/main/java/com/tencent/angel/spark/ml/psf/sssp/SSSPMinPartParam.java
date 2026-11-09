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
package com.tencent.angel.spark.ml.psf.sssp;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.update.PartIncrementRowsParam;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import io.netty.buffer.ByteBuf;
import java.util.List;

public class SSSPMinPartParam extends PartIncrementRowsParam {

  private int[] ints;

  public SSSPMinPartParam(int matrixId, PartitionKey part, List<RowUpdateSplit> updates,
      int[] ints) {
    super(matrixId, part, updates);
    this.ints = ints;
  }

  public SSSPMinPartParam() {
    this(-1, null, null, null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(ints.length);
    for (int i = 0; i < ints.length; i++) {
      buf.writeInt(ints[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len;
    len = buf.readInt();
    ints = new int[len];
    for (int i = 0; i < len; i++) {
      ints[i] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    len += ints.length * 4;
    return len;
  }

  public int[] getInts() {
    return ints;
  }

}
