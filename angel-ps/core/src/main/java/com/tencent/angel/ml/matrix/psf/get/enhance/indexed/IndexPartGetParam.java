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

package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

import java.util.Map;

/**
 * Specified index part get param class, denotes the rowId and the specified index array.
 */
public class IndexPartGetParam extends PartitionGetParam {
  private int rowId;
  private volatile int[] indexes;

  /**
   * @param matrixId matrix id
   * @param rowId row id
   * @param partKey partition key
   * @param indexes specified indexes
   */
  public IndexPartGetParam(int matrixId, int rowId, PartitionKey partKey, int[] indexes) {
    super(matrixId, partKey);
    this.rowId = rowId;
    this.indexes = indexes;
  }

  /**
   * Creates a new partition parameter.
   */
  public IndexPartGetParam() {
    this(-1, -1, null, null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(rowId);
    buf.writeInt(indexes.length);
    if (indexes.length > 0) {
      for (int i = 0; i < indexes.length; i++) {
        buf.writeInt(indexes[i]);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    this.rowId = buf.readInt();
    int len = buf.readInt();
    if (len > 0) {
      indexes = new int[len];
      for (int i = 0; i < len; i++) {
        indexes[i] = buf.readInt();
      }
    }
  }

  @Override
  public int bufferLen() {
    return 8 + 4 * indexes.length + super.bufferLen();
  }

  public int getRowId() {
    return rowId;
  }

  public int[] getIndexes() {
    return indexes;
  }

}
