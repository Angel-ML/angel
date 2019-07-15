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
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatVectorStorage;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Component int float row update split
 */
public class CompIntFloatRowUpdateSplit extends RowUpdateSplit {

  /**
   * Row update split
   */
  private final IntFloatVector split;

  /**
   * Max element number in this split
   */
  private final int maxItemNum;

  /**
   * Create a new CompIntFloatRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param split row update split
   * @param maxItemNum Max element number in this split
   */
  public CompIntFloatRowUpdateSplit(int rowIndex, IntFloatVector split, int maxItemNum) {
    super(rowIndex, RowType.T_FLOAT_DENSE, -1, -1);
    this.split = split;
    this.maxItemNum = maxItemNum;

    if (split != null) {
      IntFloatVectorStorage storage = split.getStorage();
      if (storage instanceof IntFloatDenseVectorStorage) {
        rowType = RowType.T_FLOAT_DENSE_COMPONENT;
      } else {
        rowType = RowType.T_FLOAT_SPARSE_COMPONENT;
      }
    }
  }

  /**
   * Create new empty CompIntFloatRowUpdateSplit
   */
  public CompIntFloatRowUpdateSplit() {
    this(-1, null, -1);
  }

  /**
   * Get row update split vector
   *
   * @return row update split vector
   */
  public IntFloatVector getSplit() {
    return split;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    IntFloatVectorStorage storage = split.getStorage();

    if (storage instanceof IntFloatSparseVectorStorage) {
      buf.writeInt(storage.size());
      ObjectIterator<Int2FloatMap.Entry> iter = storage.entryIterator();
      Int2FloatMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeFloat(entry.getFloatValue());
      }
    } else if (storage instanceof IntFloatSortedVectorStorage) {
      buf.writeInt(storage.size());
      int[] indices = storage.getIndices();
      float[] values = storage.getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeFloat(values[i]);
      }
    } else if (storage instanceof IntFloatDenseVectorStorage) {
      float[] values = storage.getValues();
      int writeSize = values.length < maxItemNum ? values.length : maxItemNum;
      buf.writeInt(writeSize);
      for (int i = 0; i < writeSize; i++) {
        buf.writeFloat(values[i]);
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
    if (rowType == RowType.T_FLOAT_DENSE_COMPONENT) {
      float[] values = new float[elemNum];
      for (int i = 0; i < elemNum; i++) {
        values[i] = buf.readFloat();
      }
      vector = VFactory.denseFloatVector(values);
    } else if (rowType == RowType.T_FLOAT_SPARSE_COMPONENT) {
      vector = VFactory.sparseFloatVector(
          (int) (splitContext.getPartKey().getEndCol() - splitContext.getPartKey().getStartCol()),
          elemNum);
      for (int i = 0; i < elemNum; i++) {
        ((IntFloatVector) vector).set(buf.readInt(), buf.readFloat());
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
    if (rowType == RowType.T_FLOAT_DENSE) {
      return 4 + super.bufferLen() + split.getStorage().size() * 4;
    } else {
      return 4 + super.bufferLen() + split.getStorage().size() * 8;
    }
  }
}
