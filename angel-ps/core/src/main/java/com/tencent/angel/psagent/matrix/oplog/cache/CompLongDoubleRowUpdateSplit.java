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
import com.tencent.angel.ml.math2.storage.LongDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleVectorStorage;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.utils.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Component long key double value row update split
 */
public class CompLongDoubleRowUpdateSplit extends RowUpdateSplit {

  /**
   * Row update split
   */
  private final LongDoubleVector split;

  /**
   * Create a new RowUpdateSplit.
   *
   * @param rowIndex row index
   * @param split row update split
   */
  public CompLongDoubleRowUpdateSplit(int rowIndex, LongDoubleVector split) {
    super(rowIndex, RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT, -1, -1);
    this.split = split;
  }

  /**
   * Create new empty CompLongDoubleRowUpdateSplit
   */
  public CompLongDoubleRowUpdateSplit() {
    this(-1, null);
  }

  /**
   * Get row update split vector
   *
   * @return row update split vector
   */
  public LongDoubleVector getSplit() {
    return split;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    LongDoubleVectorStorage storage = split.getStorage();
    if (storage instanceof LongDoubleSparseVectorStorage) {
      ObjectIterator<Long2DoubleMap.Entry> iter = storage.entryIterator();
      buf.writeInt(storage.size());
      Long2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    } else if (storage instanceof LongDoubleSortedVectorStorage) {
      buf.writeInt(storage.size());
      long[] indices = storage.getIndices();
      double[] values = storage.getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeDouble(values[i]);
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
    if (rowType == RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT) {
      vector = VFactory.sparseLongKeyDoubleVector(
          splitContext.getPartKey().getEndCol() - splitContext.getPartKey().getStartCol(),
          elemNum);
      for (int i = 0; i < elemNum; i++) {
        ((LongDoubleVector) vector).set(buf.readLong(), buf.readDouble());
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
    return 8 + 4 + super.bufferLen() + split.getStorage().size() * 16;
  }
}
