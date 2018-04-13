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

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Sparse double vector partition with long key.
 */
public class ServerSparseDoubleLongKeyRow extends ServerLongKeyRow{
  private final static Log LOG = LogFactory.getLog(ServerSparseDoubleLongKeyRow.class);

  /** Index->Value map */
  private volatile Long2DoubleOpenHashMap index2ValueMap;

  /**
   * Create a ServerSparseDoubleLongKeyRow
   * @param rowId row index
   * @param startCol vector partition start position
   * @param endCol vector partition end position
   */
  public ServerSparseDoubleLongKeyRow(int rowId, long startCol, long endCol, int estEleNum) {
    super(rowId, startCol, endCol);
    index2ValueMap = new Long2DoubleOpenHashMap(estEleNum);
  }

  /**
   * Create a ServerSparseDoubleLongKeyRow
   */
  public ServerSparseDoubleLongKeyRow() {
    this(0, 0, 0, 0);
  }

  @Override public RowType getRowType() {
    return RowType.T_DOUBLE_SPARSE_LONGKEY;
  }

  public Long2DoubleOpenHashMap getIndex2ValueMap() {
    return index2ValueMap;
  }

  /**
   * Set the row with Long2DoubleOpenHashMap
   * @param map the map to set.
   */
  public void setIndex2ValueMap(Long2DoubleOpenHashMap map) {
    try {
      lock.writeLock().lock();
      index2ValueMap = map;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get the default value for non-exist index
   * @return the default value for non-exist index
   */
  public double getDefaultValue() {
    return index2ValueMap.defaultReturnValue();
  }

  /**
   * Set the default value for non-exist index
   * @param value the default value for non-exist index
   */
  public void setDefaultValue(double value) {
    index2ValueMap.defaultReturnValue(value);
  }

  public void clear() {
    try {
      lock.writeLock().lock();
      index2ValueMap.clear();
      setDefaultValue(0.0);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void writeTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      super.writeTo(output);
      output.writeDouble(getDefaultValue());
      output.writeInt(index2ValueMap.size());
      for (Long2DoubleMap.Entry entry : index2ValueMap.long2DoubleEntrySet()) {
        output.writeLong(entry.getLongKey());
        output.writeDouble(entry.getDoubleValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void readFrom(DataInputStream input) throws IOException {
    try {
      lock.writeLock().lock();
      super.readFrom(input);
      double defaultVal = input.readDouble();
      int nnz = input.readInt();
      if(index2ValueMap.size() < nnz) {
        index2ValueMap = new Long2DoubleOpenHashMap(nnz);
      }
      setDefaultValue(defaultVal);

      for (int i = 0; i < nnz; i++) {
        index2ValueMap.addTo(input.readLong(), input.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int size() {
    try {
      lock.readLock().lock();
      return index2ValueMap.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void update(RowType rowType, ByteBuf buf) {
    tryToLockWrite();
    try {
      switch (rowType) {
        case T_DOUBLE_SPARSE_LONGKEY:
          updateDoubleSparse(buf);
          break;

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }

      updateRowVersion();
    } finally {
      unlockWrite();
    }
  }

  private void resizeHashMap(int size) {
    if(index2ValueMap.size() < size) {
      Long2DoubleOpenHashMap oldMap = index2ValueMap;
      index2ValueMap = new Long2DoubleOpenHashMap(size);
      setDefaultValue(oldMap.defaultReturnValue());
      index2ValueMap.putAll(oldMap);
    }
  }

  private void updateDoubleSparse(ByteBuf buf) {
    double defaultValue = buf.readDouble();
    int size = buf.readInt();
    resizeHashMap(size);
    setDefaultValue(getDefaultValue() + defaultValue);
    for (int i = 0; i < size; i++) {
      index2ValueMap.addTo(buf.readLong(), buf.readDouble());
    }
  }

  public Long2DoubleOpenHashMap getData() {
    return index2ValueMap;
  }

  @Override
  public void serialize(ByteBuf buf) {
    try {
      lock.readLock().lock();
      super.serialize(buf);
      buf.writeDouble(getDefaultValue());
      buf.writeInt(index2ValueMap.size());

      ObjectIterator<Long2DoubleMap.Entry> iter = index2ValueMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    try {
      lock.writeLock().lock();
      super.deserialize(buf);
      double defaultVal = buf.readDouble();
      int elemNum = buf.readInt();
      index2ValueMap = new Long2DoubleOpenHashMap(elemNum);
      setDefaultValue(defaultVal);
      for (int i = 0; i < elemNum; i++) {
        index2ValueMap.put(buf.readLong(), buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int bufferLen() {
    try {
      lock.readLock().lock();
      return super.bufferLen() + 4 + 8 + index2ValueMap.size() * 16;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override public void reset() {
    try {
      lock.writeLock().lock();
      index2ValueMap.clear();
      index2ValueMap.defaultReturnValue(0.0);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge a Long2DoubleOpenHashMap to this vector partition
   * @param index2ValueMap
   */
  public void mergeIndexValueMap(Long2DoubleOpenHashMap index2ValueMap) {
    try {
      lock.writeLock().lock();
      for (Map.Entry<Long, Double> entry: index2ValueMap.long2DoubleEntrySet()) {
        index2ValueMap.addTo(entry.getKey(), entry.getValue());
      }
      index2ValueMap.defaultReturnValue(index2ValueMap.defaultReturnValue() + getDefaultValue());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge in indices and values pair to this vector partition,
   * indices and values must have the same length.
   * @param indices
   * @param values
   */
  public void merge(long[] indices, double[] values) {
    assert (indices.length == values.length);
    try {
      lock.writeLock().lock();
      for (int i = 0; i < indices.length; i++) {
        index2ValueMap.addTo(indices[i], values[i]);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge a dense double vector to this vector partition
   * @param size the elements number of the dense double vector
   * @param buf  serialized dense double vector
   */
  public void mergeDoubleDense(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      resizeHashMap(size);
      for (int i = 0; i < size; i++) {
        index2ValueMap.addTo(i, buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }


  /**
   * Merge a sparse double vector to this vector partition
   * @param size the elements number of the sparse double vector
   * @param buf serialized sparse double vector
   */
  public void mergeDoubleSparse(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      resizeHashMap(size);
      setDefaultValue(buf.readDouble());
      for (int i = 0; i < size; i++) {
        index2ValueMap.addTo(buf.readLong(), buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Put all elements in this vector partition to a given map
   * @param other a long->double map
   */
  public void mergeTo(Long2DoubleOpenHashMap other) {
    try {
      lock.readLock().lock();
      other.putAll(index2ValueMap);
      setDefaultValue(other.defaultReturnValue() + getDefaultValue());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Put the elements in this vector partition to the given index and value arrays
   * @param indexes index array
   * @param values value array
   * @param startPos the start position for the elements of this vector partition
   * @param len the reserved length for this vector partition
   */
  public void mergeTo(long[] indexes, double[] values, int startPos, int len) {
    try {
      lock.readLock().lock();
      int writeLen = len < index2ValueMap.size() ? len : index2ValueMap.size();
      if (writeLen == 0) {
        return;
      }

      int index = 0;

      ObjectIterator<Long2DoubleMap.Entry> iter = index2ValueMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexes[startPos + index] = entry.getLongKey();
        values[startPos + index] = entry.getDoubleValue();
        index++;
        if (index == writeLen) {
          return;
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override public void getValues(long[] indexes, ByteBuf buffer) {
    try {
      lock.readLock().lock();
      int len = indexes.length;
      for(int i = 0; i < len; i++) {
        buffer.writeDouble(index2ValueMap.get(indexes[i]));
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Batch get values use indexes
   * @param indexes elements indexes
   * @return element values
   */
  public double[] getValues(long[] indexes) {
    double [] values = new double[indexes.length];
    try {
      lock.readLock().lock();
      int len = indexes.length;
      for(int i = 0; i < len; i++) {
        values[i] = index2ValueMap.get(indexes[i]);
      }
      return values;
    } finally {
      lock.readLock().unlock();
    }
  }
}
