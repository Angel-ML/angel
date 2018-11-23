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
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Component int key int row update split
 */
public class CompIntLongRowUpdateSplit extends RowUpdateSplit {

  /**
   * Row update split
   */
  private final IntLongVector split;

  /**
   * Max element number in this split
   */
  private final int maxItemNum;

  /**
   * Create a new CompIntIntRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param split row update split
   */
  public CompIntLongRowUpdateSplit(int rowIndex, IntLongVector split, int maxItemNum) {
    super(rowIndex, RowType.T_LONG_DENSE, -1, -1);
    this.split = split;
    this.maxItemNum = maxItemNum;

    IntLongVectorStorage storage = split.getStorage();
    if (storage instanceof IntLongDenseVectorStorage) {
      rowType = RowType.T_LONG_DENSE_COMPONENT;
    } else {
      rowType = RowType.T_LONG_SPARSE_COMPONENT;
    }
  }

  public IntLongVector getSplit() {
    return split;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    IntLongVectorStorage storage = split.getStorage();

    if (storage instanceof IntLongSparseVectorStorage) {
      buf.writeInt(storage.size());
      ObjectIterator<Int2LongMap.Entry> iter = storage.entryIterator();
      Int2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeLong(entry.getLongValue());
      }
    } else if (storage instanceof IntLongSortedVectorStorage) {
      buf.writeInt(storage.size());
      int[] indices = storage.getIndices();
      long[] values = storage.getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeLong(values[i]);
      }
    } else if (storage instanceof IntLongDenseVectorStorage) {
      long[] values = storage.getValues();
      int writeSize = values.length < maxItemNum ? values.length : maxItemNum;
      buf.writeInt(writeSize);
      for (int i = 0; i < writeSize; i++) {
        buf.writeLong(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "unsupport split for storage " + storage.getClass().getName());
    }
  }

  @Override
  public long size() {
    return split.size();
  }

  @Override
  public int bufferLen() {
    if (rowType == RowType.T_LONG_DENSE) {
      return 4 + super.bufferLen() + split.getStorage().size() * 8;
    } else {
      return 4 + super.bufferLen() + split.getStorage().size() * 12;
    }
  }
}
