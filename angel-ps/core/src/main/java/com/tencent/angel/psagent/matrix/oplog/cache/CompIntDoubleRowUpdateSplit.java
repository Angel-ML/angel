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
import com.tencent.angel.ml.math2.storage.IntDoubleDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Component int double row update split
 */
public class CompIntDoubleRowUpdateSplit extends RowUpdateSplit {

  /**
   * Row update split, it only use in serialization
   */
  private final IntDoubleVector split;

  /**
   * Max element number in this split
   */
  private final int maxItemNum;

  /**
   * Create a new CompIntDoubleRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param split row update split
   * @param maxItemNum Max element number in this split
   */
  public CompIntDoubleRowUpdateSplit(int rowIndex, IntDoubleVector split, int maxItemNum) {
    super(rowIndex, RowType.T_DOUBLE_DENSE, -1, -1);
    this.split = split;
    this.maxItemNum = maxItemNum;

    if (split != null) {
      IntDoubleVectorStorage storage = split.getStorage();
      if (storage instanceof IntDoubleDenseVectorStorage) {
        rowType = RowType.T_DOUBLE_DENSE_COMPONENT;
      } else {
        rowType = RowType.T_DOUBLE_SPARSE_COMPONENT;
      }
    }
  }

  /**
   * Create a empty CompIntDoubleRowUpdateSplit.
   */
  public CompIntDoubleRowUpdateSplit() {
    this(-1, null, -1);
  }

  /**
   * Get row update split vector
   *
   * @return row update split vector
   */
  public IntDoubleVector getSplit() {
    return split;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    IntDoubleVectorStorage storage = split.getStorage();

    if (storage instanceof IntDoubleSparseVectorStorage) {
      buf.writeInt(storage.size());
      ObjectIterator<Int2DoubleMap.Entry> iter = storage.entryIterator();
      Int2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    } else if (storage instanceof IntDoubleSortedVectorStorage) {
      buf.writeInt(storage.size());
      int[] indices = storage.getIndices();
      double[] values = storage.getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeDouble(values[i]);
      }
    } else if (storage instanceof IntDoubleDenseVectorStorage) {
      double[] values = storage.getValues();
      int writeSize = values.length < maxItemNum ? values.length : maxItemNum;
      buf.writeInt(writeSize);
      for (int i = 0; i < writeSize; i++) {
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
    if (rowType == RowType.T_DOUBLE_DENSE_COMPONENT) {
      double[] values = new double[elemNum];
      for (int i = 0; i < elemNum; i++) {
        values[i] = buf.readDouble();
      }
      vector = VFactory.denseDoubleVector(values);
    } else if (rowType == RowType.T_DOUBLE_SPARSE_COMPONENT) {
      vector = VFactory.sparseDoubleVector(
          (int) (splitContext.getPartKey().getEndCol() - splitContext.getPartKey().getStartCol()),
          elemNum);
      for (int i = 0; i < elemNum; i++) {
        ((IntDoubleVector) vector).set(buf.readInt(), buf.readDouble());
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
    if (rowType == RowType.T_DOUBLE_DENSE) {
      return 4 + super.bufferLen() + split.getStorage().size() * 8;
    } else {
      return 4 + super.bufferLen() + split.getStorage().size() * 12;
    }
  }
}
