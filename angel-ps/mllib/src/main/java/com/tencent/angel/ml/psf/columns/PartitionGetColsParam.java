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


package com.tencent.angel.ml.psf.columns;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ps.server.data.request.InitFunc;
import io.netty.buffer.ByteBuf;

public class PartitionGetColsParam extends PartitionGetParam {

  public long[] cols;
  public int[] rows;
  public InitFunc func;

  public PartitionGetColsParam(int matId, PartitionKey pkey, int[] rows, long[] cols, InitFunc func) {
    super(matId, pkey);
    this.rows = rows;
    this.cols = cols;
    this.func = func;
  }

  public PartitionGetColsParam() {
    super();
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeBoolean(func != null);
    if(func != null) {
      byte[] data = func.getClass().getName().getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
      func.serialize(buf);
    }

    buf.writeInt(rows.length);
    for (int i = 0; i < rows.length; i++)
      buf.writeInt(rows[i]);
    buf.writeInt(cols.length);
    for (int i = 0; i < cols.length; i++)
      buf.writeLong(cols[i]);
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    boolean useInitFunc = buf.readBoolean();
    if(useInitFunc) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      String initFuncClass = new String(data);
      try {
        func = (InitFunc) Class.forName(initFuncClass).newInstance();
      } catch (Throwable e) {
        throw new UnsupportedOperationException(e);
      }
      func.deserialize(buf);
    }

    int nRows = buf.readInt();
    rows = new int[nRows];
    for (int i = 0; i < nRows; i++)
      rows[i] = buf.readInt();
    int nCols = buf.readInt();
    cols = new long[nCols];
    for (int i = 0; i < nCols; i++)
      cols[i] = buf.readLong();
  }

  @Override public int bufferLen() {
    int len = super.bufferLen() + rows.length * 4 + cols.length * 8 + 8;
    if(func != null) {
      len += (4 + 4 + func.getClass().getName().getBytes().length + func.bufferLen());
    } else {
      len += 4;
    }
    return len;
  }
}
