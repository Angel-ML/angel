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

import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.storage.LongLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongVectorStorage;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.utils.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Component long key int value row update split
 */
public class CompLongLongRowUpdateSplit extends RowUpdateSplit {

  /**
   * Row update split
   */
  private final LongLongVector split;

  /**
   * Create a new CompLongIntRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param split update split
   */
  public CompLongLongRowUpdateSplit(int rowIndex, LongLongVector split) {
    super(rowIndex, RowType.T_LONG_SPARSE_LONGKEY_COMPONENT, -1, -1);
    this.split = split;
  }

  /**
   * Create new empty CompLongLongRowUpdateSplit
   */
  public CompLongLongRowUpdateSplit() {
    this(-1, null);
  }

  /**
   * Get row update split vector
   *
   * @return row update split vector
   */
  public LongLongVector getSplit() {
    return split;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    LongLongVectorStorage storage = split.getStorage();
    buf.writeInt(storage.size());
    if (storage instanceof LongLongSparseVectorStorage) {
      ObjectIterator<Long2LongMap.Entry> iter = storage.entryIterator();
      Long2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeLong(entry.getLongValue());
      }
    } else if (storage instanceof LongLongSortedVectorStorage) {
      long[] indices = storage.getIndices();
      long[] values = storage.getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeLong(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "unsupport split for storage " + storage.getClass().getName());
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int elemNum = buf.readInt();
    if (rowType == RowType.T_LONG_SPARSE_LONGKEY_COMPONENT) {
      vector = VFactory.sparseLongKeyLongVector(
          splitContext.getPartKey().getEndCol() - splitContext.getPartKey().getStartCol(),
          elemNum);
      for (int i = 0; i < elemNum; i++) {
        ((LongLongVector) vector).set(buf.readLong(), buf.readLong());
      }
    } else {
      throw new UnsupportedOperationException("Unsupport rowtype " + rowType);
    }
  }

  @Override
  public long size() {
    return split.size();
  }

  @Override
  public int bufferLen() {
    return 4 + super.bufferLen() + split.getStorage().size() * 16;
  }
}
