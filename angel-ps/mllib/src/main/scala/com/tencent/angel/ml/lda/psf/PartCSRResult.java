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


package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
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
      if (row.isDense())
        serializeDense(buf, (ServerIntIntRow) row);
      else if (row.isSparse())
        serializeSparse(buf, (ServerIntIntRow) row);
      else
        throw new AngelException("LDA should be set with ServerDenseIntRow");

    }
  }

  public void serializeDense(ByteBuf buf, ServerIntIntRow row) {

    try {
      row.startRead();
      int[] values = row.getValues();
      int len = (int) (row.getEndCol() - row.getStartCol());
      int cnt = 0;
      for (int i = 0; i < len; i++)
        if (values[i] > 0)
          cnt++;

      if (cnt > len * 0.5) {
        // dense
        buf.writeByte(0);
        buf.writeInt(len);
        for (int i = 0; i < len; i++)
          buf.writeInt(values[i]);
      } else {
        // sparse
        buf.writeByte(1);
        buf.writeInt(cnt);
        for (int i = 0; i < len; i++) {
          if (values[i] > 0) {
            buf.writeInt(i);
            buf.writeInt(values[i]);
          }
        }
      }
    } finally {
      row.endRead();
    }

  }

  public void serializeSparse(ByteBuf buf, ServerIntIntRow row) {
    try {
      row.startRead();
      ObjectIterator<Int2IntMap.Entry> iterator = row.getIter();
      buf.writeByte(1);

      int index = buf.writerIndex();
      buf.writeInt(row.size());

      int cnt = 0;
      while (iterator.hasNext()) {
        Int2IntMap.Entry entry = iterator.next();
        int key = entry.getIntKey();
        int val = entry.getIntValue();
        if (val > 0) {
          buf.writeInt(key);
          buf.writeInt(val);
          cnt++;
        }
      }

      buf.setInt(index, cnt);
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
        len = buf.readInt();
        for (int i = 0; i < len; i++)
          row[i] = buf.readInt();
        break;
      case 1:
        // sparse
        len = buf.readInt();
        Arrays.fill(row, 0);
        for (int i = 0; i < len; i++) {
          int key = buf.readInt();
          int val = buf.readInt();
          row[key] = val;
        }
        break;
      default:
        throw new AngelException("type mismatch");
    }
    return true;
  }

  public void clear() {
    try {
      buf.release();
    } catch (Throwable x) {

    }
  }
}
