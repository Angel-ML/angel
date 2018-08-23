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
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Component int key int row update split
 */
public class CompIntIntRowUpdateSplit extends RowUpdateSplit {
  /**
   * Row update split
   */
  private final IntIntVector split;

  /**
   * Create a new CompIntIntRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param split    row update split
   */
  public CompIntIntRowUpdateSplit(int rowIndex, IntIntVector split) {
    super(rowIndex, RowType.T_INT_DENSE, -1, -1);
    this.split = split;
    IntIntVectorStorage storage = split.getStorage();
    if (storage instanceof IntIntDenseVectorStorage) {
      rowType = RowType.T_INT_DENSE_COMPONENT;
    } else {
      rowType = RowType.T_INT_SPARSE_COMPONENT;
    }
  }

  public IntIntVector getSplit() {
    return split;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    IntIntVectorStorage storage = split.getStorage();
    buf.writeInt(storage.size());
    if (storage instanceof IntIntSparseVectorStorage) {
      ObjectIterator<Int2IntMap.Entry> iter = storage.entryIterator();
      Int2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeInt(entry.getIntValue());
      }
    } else if (storage instanceof IntIntSortedVectorStorage) {
      int[] indices = storage.getIndices();
      int[] values = storage.getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeInt(values[i]);
      }
    } else if (storage instanceof IntIntDenseVectorStorage) {
      int[] values = storage.getValues();
      for (int i = 0; i < values.length; i++) {
        buf.writeInt(values[i]);
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
    if (rowType == RowType.T_INT_DENSE) {
      return 4 + super.bufferLen() + split.getStorage().size() * 4;
    } else {
      return 4 + super.bufferLen() + split.getStorage().size() * 8;
    }
  }
}
