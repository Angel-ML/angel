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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LongIndexPartGetParam extends PartitionGetParam {

  private final static Log LOG = LogFactory.getLog(LongIndexPartGetParam.class);
  private int rowId;
  private long[] index;

  /**
   * @param matrixId matrix id
   * @param rowId row id
   * @param partKey partition key
   * @param index specified index
   */
  public LongIndexPartGetParam(int matrixId, int rowId, PartitionKey partKey, long[] index) {
    super(matrixId, partKey);
    this.rowId = rowId;
    this.index = index;
  }

  /**
   * Creates a new partition parameter.
   */
  public LongIndexPartGetParam() {
    this(-1, -1, null, null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(rowId);
    buf.writeInt(index.length);

    if (index.length > 0) {
      for(int i = 0; i < index.length; i++) {
        buf.writeLong(index[i]);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    this.rowId = buf.readInt();
    int len = buf.readInt();

    if (len > 0) {
      index = new long[len];
      for (int i = 0; i < len; i++) {
        index[i] = buf.readLong();
      }
    }
  }

  @Override
  public int bufferLen() {
    return 8 + super.bufferLen() + 8 * index.length;
  }

  public int getRowId() {
    return rowId;
  }

  public long[] getIndex() {
    return index;
  }
}
