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

package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

/**
 * Specified index part get param class, denotes the rowId and the specified index array.
 */
public class IndexPartGetParam extends PartitionGetParam {
  private int rowId;
  private int paramId;
  private int[] index;

  /**
   * @param matrixId matrix id
   * @param rowId row id
   * @param partKey partition key
   * @param index specified index
   * @param paramId the part param id
   */
  public IndexPartGetParam(int matrixId, int rowId, PartitionKey partKey, int[] index,
                                    int paramId) {
    super(matrixId, partKey);
    this.rowId = rowId;
    this.index = index;
    this.paramId = paramId;
  }

  /**
   * Creates a new partition parameter.
   */
  public IndexPartGetParam() {
    this(-1, -1, null, null, -1);
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(matrixId);
    if (partKey != null)
      partKey.serialize(buf);

    buf.writeInt(rowId);
    buf.writeInt(paramId);
    buf.writeInt(index.length);

    if (index.length > 0) {
      for (int id : index) {
        buf.writeInt(id);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    matrixId = buf.readInt();
    if (buf.isReadable()) {
      if (partKey == null) {
        partKey = new PartitionKey();
      }

      partKey.deserialize(buf);
    }

    this.rowId = buf.readInt();
    this.paramId = buf.readInt();
    int len = buf.readInt();

    if (len > 0) {
      index = new int[len];
      for (int i = 0; i < len; i++) {
        index[i] = buf.readInt();
      }
    }
  }

  @Override
  public int bufferLen() {
    return 16 + 8 * index.length;
  }

  public int getRowId() {
    return rowId;
  }

  public int[] getIndex() {
    return index;
  }

  public int getParamId() {
    return paramId;
  }
}
