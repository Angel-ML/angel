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
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PartitionUpdateColsParam extends PartitionUpdateParam {

  private static Log LOG = LogFactory.getLog(PartitionUpdateColsParam.class);

  int[] rows;
  long[] cols;
  Vector vector;
  UpdateOp op;

  public PartitionUpdateColsParam(int matId, PartitionKey pkey, int[] rows, long[] cols,
    Vector vector, UpdateOp op) {
    super(matId, pkey, false);
    this.rows = rows;
    this.cols = cols;
    this.vector = vector;
    this.op = op;
  }

  public PartitionUpdateColsParam() {
    super();
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(this.op.getOpId());

    buf.writeInt(rows.length);
    for (int i = 0; i < rows.length; i++)
      buf.writeInt(rows[i]);
    buf.writeInt(cols.length);
    PartitionGetColsResult.serialize(buf, cols, vector);
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.op = UpdateOp.valueOf(buf.readInt());

    int nRows = buf.readInt();
    rows = new int[nRows];
    for (int i = 0; i < nRows; i++)
      rows[i] = buf.readInt();
    int nCols = buf.readInt();
    cols = new long[nCols];
    vector = PartitionGetColsResult.deserialize(buf, rows, cols);
  }

  @Override public int bufferLen() {
    int len = super.bufferLen();
    len += 4 + PartitionGetColsResult.bufferLen(rows, cols, vector);
    return len;
  }
}
