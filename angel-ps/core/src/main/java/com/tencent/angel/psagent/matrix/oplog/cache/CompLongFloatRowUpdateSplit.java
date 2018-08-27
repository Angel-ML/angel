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


package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Component long key float value row update split
 */
public class CompLongFloatRowUpdateSplit extends RowUpdateSplit {
  /**
   * Row update split
   */
  private final LongFloatVector split;

  /**
   * Create a new CompLongFloatRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param split    update split
   */
  public CompLongFloatRowUpdateSplit(int rowIndex, LongFloatVector split) {
    super(rowIndex, RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT, -1, -1);
    this.split = split;
  }

  public LongFloatVector getSplit() {
    return split;
  }

  @Override public void serialize(ByteBuf buf) {
    // TODO:
    super.serialize(buf);
    LongFloatVectorStorage storage = split.getStorage();
    buf.writeInt(storage.size());
    if (storage instanceof LongFloatSparseVectorStorage) {
      ObjectIterator<Long2FloatMap.Entry> iter = storage.entryIterator();
      Long2FloatMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeFloat(entry.getFloatValue());
      }
    } else if (storage instanceof LongFloatSortedVectorStorage) {
      long[] indices = storage.getIndices();
      float[] values = storage.getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeFloat(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage " + storage.getClass().getName());
    }
  }

  @Override public long size() {
    return split.size();
  }

  @Override public int bufferLen() {
    return 4 + super.bufferLen() + split.getStorage().size() * 12;
  }
}
