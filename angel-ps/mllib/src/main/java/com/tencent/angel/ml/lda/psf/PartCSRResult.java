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

package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.List;

public class PartCSRResult extends PartitionGetResult {

  private static final Log LOG = LogFactory.getLog(PartCSRResult.class);

  private List<ServerRow> splits;
  private ByteBuf buf;
  private int len;
  private int readerIdx;

  public PartCSRResult(List<ServerRow> splits) {
    this.splits = splits;
  }

  public PartCSRResult() {
  }

  @Override public void serialize(ByteBuf buf) {
    // Write #rows
    buf.writeInt(splits.size());
    // Write each row
    for (ServerRow row : splits) {
      if (row instanceof ServerDenseIntRow) {
        serialize(buf, (ServerDenseIntRow) row);
      } else {
        throw new AngelException("LDA should be set with ServerDenseIntRow");
      }
    }
  }

  public void serialize(ByteBuf buf, ServerDenseIntRow row) {

    try {
      row.getLock().readLock().lock();
      IntBuffer ints = row.getData();
      int len = (int) (row.getEndCol() - row.getStartCol());
      int cnt = 0;
      for (int i = 0; i < len; i++)
        if (ints.get(i) > 0)
          cnt++;

      if (cnt > len * 0.5) {
        // dense
        buf.writeByte(0);
        buf.writeShort(len);
        buf.writeBytes(row.getDataArray());
      } else {
        // sparse
        buf.writeByte(1);
        buf.writeShort(cnt);
        for (int i = 0; i < len; i++) {
          if (ints.get(i) > 0) {
            buf.writeShort(i);
            buf.writeInt(ints.get(i));
          }
        }
      }
    } finally {
      row.getLock().readLock().unlock();
    }

  }

  @Override public void deserialize(ByteBuf buf) {
    this.len = buf.readInt();
    this.buf = buf.duplicate();
    this.buf.retain();
    //    LOG.info(buf.refCnt());
    this.readerIdx = 0;
  }

  @Override public int bufferLen() {
    return 16;
  }

  public boolean read(int[] row) {
    if (readerIdx == len)
      return false;

    readerIdx++;

    int type = buf.readByte();
    int len;
    switch (type) {
      case 0:
        // dense
        len = buf.readShort();
        for (int i = 0; i < len; i++)
          row[i] = buf.readInt();
        break;
      case 1:
        // sparse
        len = buf.readShort();
        Arrays.fill(row, 0);
        for (int i = 0; i < len; i++) {
          int key = buf.readShort();
          int val = buf.readInt();
          row[key] = val;
        }
        break;
      default:
        throw new AngelException("type mismatch");
    }

    if (readerIdx == this.len) {
      buf.release();
      //      LOG.info(buf.refCnt());
    }
    return true;
  }

}
