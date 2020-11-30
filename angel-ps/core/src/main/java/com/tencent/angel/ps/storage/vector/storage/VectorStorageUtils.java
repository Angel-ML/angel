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

package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ml.math2.vector.DoubleVector;
import com.tencent.angel.ml.math2.vector.FloatVector;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntKeyVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.IntVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.LongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Vector storage utils
 */
public class VectorStorageUtils {

  protected Vector vector;

  public static boolean useIntKey(Vector vector) {
    return vector instanceof IntKeyVector;
  }

  public static boolean isDense(Vector vector) {
    return vector.isDense();
  }

  public static boolean isSparse(Vector vector) {
    return vector.isSparse();
  }

  public static boolean isSorted(Vector vector) {
    return vector.isSorted();
  }

  public static BasicType getKeyType(Vector vector) {
    if (vector instanceof IntKeyVector) {
      return BasicType.INT;
    } else {
      return BasicType.LONG;
    }
  }

  public static BasicType getValueType(Vector vector) {
    if (vector instanceof IntVector) {
      return BasicType.INT;
    } else if (vector instanceof LongVector) {
      return BasicType.LONG;
    } else if (vector instanceof FloatVector) {
      return BasicType.FLOAT;
    } else {
      return BasicType.DOUBLE;
    }
  }

  public static StorageMethod getStorageMethod(Vector vector) {
    if (vector.isDense()) {
      return StorageMethod.DENSE;
    } else if (vector.isSparse()) {
      return StorageMethod.SPARSE;
    } else if (vector.isSorted()) {
      return StorageMethod.SORTED;
    } else {
      return StorageMethod.SPARSE;
    }
  }

  public static void serialize(ByteBuf buf, Vector vector) {
    // Row type
    buf.writeInt(vector.getType().getNumber());

    // Storage method
    buf.writeInt(getStorageMethod(vector).getValue());

    // Key type
    buf.writeInt(getKeyType(vector).getValue());

    // Value type
    buf.writeInt(getValueType(vector).getValue());

    // Vector dim
    buf.writeLong(vector.dim());

    // Vector length
    buf.writeLong(vector.getSize());

    // Vector data
    serializeVector(buf, vector);
  }

  public static Vector deserialize(ByteBuf buf) {
    // Row type
    RowType rowType = RowType.valueOf(buf.readInt());

    // Storage method
    StorageMethod storageMethod = StorageMethod.valuesOf(buf.readInt());

    // Key type
    BasicType keyType = BasicType.valuesOf(buf.readInt());

    // Value type
    BasicType valueType = BasicType.valuesOf(buf.readInt());

    // Vector dim
    long dim = buf.readLong();

    // Vector length
    long len = buf.readLong();

    // Init the vector
    Vector vector = VectorFactory.getVector(rowType, storageMethod, keyType, valueType, dim, len);

    // Vector data
    deserializeVector(buf, vector);

    return vector;
  }


  public static void serializeVector(ByteBuf buf, Vector vector) {
    if (vector instanceof IntVector) {
      serializeVector(buf, (IntVector) vector);
    } else if (vector instanceof LongVector) {
      serializeVector(buf, (LongVector) vector);
    } else if (vector instanceof FloatVector) {
      serializeVector(buf, (FloatVector) vector);
    } else {
      serializeVector(buf, (DoubleVector) vector);
    }
  }

  public static void serializeVector(ByteBuf buf, IntVector vector) {
    if (vector instanceof IntIntVector) {
      serializeVector(buf, (IntIntVector) vector);
    } else {
      serializeVector(buf, (LongIntVector) vector);
    }
  }

  public static void serializeVector(ByteBuf buf, LongVector vector) {
    if (vector instanceof IntLongVector) {
      serializeVector(buf, (IntLongVector) vector);
    } else {
      serializeVector(buf, (LongLongVector) vector);
    }
  }

  public static void serializeVector(ByteBuf buf, FloatVector vector) {
    if (vector instanceof IntFloatVector) {
      serializeVector(buf, (IntFloatVector) vector);
    } else {
      serializeVector(buf, (LongFloatVector) vector);
    }
  }

  public static void serializeVector(ByteBuf buf, DoubleVector vector) {
    if (vector instanceof IntDoubleVector) {
      serializeVector(buf, (IntDoubleVector) vector);
    } else {
      serializeVector(buf, (LongDoubleVector) vector);
    }
  }

  public static void serializeVector(ByteBuf buf, IntIntVector vector) {
    StorageMethod method = getStorageMethod(vector);
    if (method == StorageMethod.SPARSE) {
      // Sparse storage, use the iterator to avoid array copy
      ObjectIterator<Entry> iter = vector.getStorage().entryIterator();
      int sizeIndex = buf.writerIndex();
      buf.writeInt(0);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      Int2IntMap.Entry entry;
      int elemNum = 0;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeInt(entry.getIntValue());
        elemNum++;
      }
      buf.setInt(sizeIndex, elemNum);
    } else if (method == StorageMethod.SORTED) {
      // Get the array pair
      int[] indices = vector.getStorage().getIndices();
      int[] values = vector.getStorage().getValues();
      buf.writeInt(indices.length);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeInt(values[i]);
      }
    } else if (method == StorageMethod.DENSE) {
      int[] values = vector.getStorage().getValues();
      buf.writeInt(values.length);
      buf.writeInt(SerializeArrangement.VALUE.getValue());
      for (int i = 0; i < values.length; i++) {
        buf.writeInt(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unknown vector storage type:" + vector.getStorage().getClass().getName());
    }
  }

  public static void serializeVector(ByteBuf buf, IntLongVector vector) {
    StorageMethod method = getStorageMethod(vector);
    if (method == StorageMethod.SPARSE) {
      // Sparse storage, use the iterator to avoid array copy
      ObjectIterator<Int2LongMap.Entry> iter = vector.getStorage().entryIterator();
      int sizeIndex = buf.writerIndex();
      buf.writeInt(0);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      Int2LongMap.Entry entry;
      int elemNum = 0;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeLong(entry.getLongValue());
        elemNum++;
      }
      buf.setInt(sizeIndex, elemNum);
    } else if (method == StorageMethod.SORTED) {
      // Get the array pair
      int[] indices = vector.getStorage().getIndices();
      long[] values = vector.getStorage().getValues();
      buf.writeInt(indices.length);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeLong(values[i]);
      }
    } else if (method == StorageMethod.DENSE) {
      long[] values = vector.getStorage().getValues();
      buf.writeInt(values.length);
      buf.writeInt(SerializeArrangement.VALUE.getValue());
      for (int i = 0; i < values.length; i++) {
        buf.writeLong(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unknown vector storage type:" + vector.getStorage().getClass().getName());
    }
  }

  public static void serializeVector(ByteBuf buf, IntFloatVector vector) {
    StorageMethod method = getStorageMethod(vector);
    if (method == StorageMethod.SPARSE) {
      // Sparse storage, use the iterator to avoid array copy
      ObjectIterator<Int2FloatMap.Entry> iter = vector.getStorage().entryIterator();
      int sizeIndex = buf.writerIndex();
      buf.writeInt(0);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      Int2FloatMap.Entry entry;
      int elemNum = 0;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeFloat(entry.getFloatValue());
        elemNum++;
      }
      buf.setInt(sizeIndex, elemNum);
    } else if (method == StorageMethod.SORTED) {
      // Get the array pair
      int[] indices = vector.getStorage().getIndices();
      float[] values = vector.getStorage().getValues();
      buf.writeInt(indices.length);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeFloat(values[i]);
      }
    } else if (method == StorageMethod.DENSE) {
      float[] values = vector.getStorage().getValues();
      buf.writeInt(values.length);
      buf.writeInt(SerializeArrangement.VALUE.getValue());
      for (int i = 0; i < values.length; i++) {
        buf.writeFloat(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unknown vector storage type:" + vector.getStorage().getClass().getName());
    }
  }

  public static void serializeVector(ByteBuf buf, IntDoubleVector vector) {
    StorageMethod method = getStorageMethod(vector);
    if (method == StorageMethod.SPARSE) {
      // Sparse storage, use the iterator to avoid array copy
      ObjectIterator<Int2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
      int sizeIndex = buf.writerIndex();
      buf.writeInt(0);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      Int2DoubleMap.Entry entry;
      int elemNum = 0;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeDouble(entry.getDoubleValue());
        elemNum++;
      }
      buf.setInt(sizeIndex, elemNum);
    } else if (method == StorageMethod.SORTED) {
      // Get the array pair
      int[] indices = vector.getStorage().getIndices();
      double[] values = vector.getStorage().getValues();
      buf.writeInt(indices.length);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeDouble(values[i]);
      }
    } else if (method == StorageMethod.DENSE) {
      double[] values = vector.getStorage().getValues();
      buf.writeInt(values.length);
      buf.writeInt(SerializeArrangement.VALUE.getValue());
      for (int i = 0; i < values.length; i++) {
        buf.writeDouble(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unknown vector storage type:" + vector.getStorage().getClass().getName());
    }
  }

  public static void serializeVector(ByteBuf buf, LongIntVector vector) {
    StorageMethod method = getStorageMethod(vector);
    if (method == StorageMethod.SPARSE) {
      // Sparse storage, use the iterator to avoid array copy
      ObjectIterator<Long2IntMap.Entry> iter = vector.getStorage().entryIterator();
      int sizeIndex = buf.writerIndex();
      buf.writeInt(0);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      Long2IntMap.Entry entry;
      int elemNum = 0;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeInt(entry.getIntValue());
        elemNum++;
      }
      buf.setInt(sizeIndex, elemNum);
    } else if (method == StorageMethod.SORTED) {
      // Get the array pair
      long[] indices = vector.getStorage().getIndices();
      int[] values = vector.getStorage().getValues();
      buf.writeInt(indices.length);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeInt(values[i]);
      }
    } else if (method == StorageMethod.DENSE) {
      // Impossible now
      int[] values = vector.getStorage().getValues();
      buf.writeInt(values.length);
      buf.writeInt(SerializeArrangement.VALUE.getValue());
      for (int i = 0; i < values.length; i++) {
        buf.writeInt(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unknown vector storage type:" + vector.getStorage().getClass().getName());
    }
  }

  public static void serializeVector(ByteBuf buf, LongLongVector vector) {
    StorageMethod method = getStorageMethod(vector);
    if (method == StorageMethod.SPARSE) {
      // Sparse storage, use the iterator to avoid array copy
      ObjectIterator<Long2LongMap.Entry> iter = vector.getStorage().entryIterator();
      int sizeIndex = buf.writerIndex();
      buf.writeInt(0);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      Long2LongMap.Entry entry;
      int elemNum = 0;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeLong(entry.getLongValue());
        elemNum++;
      }
      buf.setInt(sizeIndex, elemNum);
    } else if (method == StorageMethod.SORTED) {
      // Get the array pair
      long[] indices = vector.getStorage().getIndices();
      long[] values = vector.getStorage().getValues();
      buf.writeInt(indices.length);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeLong(values[i]);
      }
    } else if (method == StorageMethod.DENSE) {
      // Impossible now
      long[] values = vector.getStorage().getValues();
      buf.writeInt(values.length);
      buf.writeInt(SerializeArrangement.VALUE.getValue());
      for (int i = 0; i < values.length; i++) {
        buf.writeLong(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unknown vector storage type:" + vector.getStorage().getClass().getName());
    }
  }

  public static void serializeVector(ByteBuf buf, LongFloatVector vector) {
    StorageMethod method = getStorageMethod(vector);
    if (method == StorageMethod.SPARSE) {
      // Sparse storage, use the iterator to avoid array copy
      ObjectIterator<Long2FloatMap.Entry> iter = vector.getStorage().entryIterator();
      int sizeIndex = buf.writerIndex();
      buf.writeInt(0);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      Long2FloatMap.Entry entry;
      int elemNum = 0;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeFloat(entry.getFloatValue());
        elemNum++;
      }
      buf.setInt(sizeIndex, elemNum);
    } else if (method == StorageMethod.SORTED) {
      // Get the array pair
      long[] indices = vector.getStorage().getIndices();
      float[] values = vector.getStorage().getValues();
      buf.writeInt(indices.length);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeFloat(values[i]);
      }
    } else if (method == StorageMethod.DENSE) {
      // Impossible now
      float[] values = vector.getStorage().getValues();
      buf.writeInt(values.length);
      buf.writeInt(SerializeArrangement.VALUE.getValue());
      for (int i = 0; i < values.length; i++) {
        buf.writeFloat(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unknown vector storage type:" + vector.getStorage().getClass().getName());
    }
  }


  public static void serializeVector(ByteBuf buf, LongDoubleVector vector) {
    StorageMethod method = getStorageMethod(vector);
    if (method == StorageMethod.SPARSE) {
      // Sparse storage, use the iterator to avoid array copy
      ObjectIterator<Long2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
      int sizeIndex = buf.writerIndex();
      buf.writeInt(0);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      Long2DoubleMap.Entry entry;
      int elemNum = 0;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeDouble(entry.getDoubleValue());
        elemNum++;
      }
      buf.setInt(sizeIndex, elemNum);
    } else if (method == StorageMethod.SORTED) {
      // Get the array pair
      long[] indices = vector.getStorage().getIndices();
      double[] values = vector.getStorage().getValues();
      buf.writeInt(indices.length);
      buf.writeInt(SerializeArrangement.KEY_VALUE.getValue());
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeDouble(values[i]);
      }
    } else if (method == StorageMethod.DENSE) {
      // Impossible now
      double[] values = vector.getStorage().getValues();
      buf.writeInt(values.length);
      buf.writeInt(SerializeArrangement.VALUE.getValue());
      for (int i = 0; i < values.length; i++) {
        buf.writeDouble(values[i]);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unknown vector storage type:" + vector.getStorage().getClass().getName());
    }
  }

  public static void deserializeVector(ByteBuf buf, Vector vector) {
    if (vector instanceof IntVector) {
      deserializeVector(buf, (IntVector) vector);
    } else if (vector instanceof LongVector) {
      deserializeVector(buf, (LongVector) vector);
    } else if (vector instanceof FloatVector) {
      deserializeVector(buf, (FloatVector) vector);
    } else {
      deserializeVector(buf, (DoubleVector) vector);
    }
  }

  public static void deserializeVector(ByteBuf buf, IntVector vector) {
    if (vector instanceof IntIntVector) {
      deserializeVector(buf, (IntIntVector) vector);
    } else {
      deserializeVector(buf, (LongIntVector) vector);
    }
  }

  public static void deserializeVector(ByteBuf buf, LongVector vector) {
    if (vector instanceof IntLongVector) {
      deserializeVector(buf, (IntLongVector) vector);
    } else {
      deserializeVector(buf, (LongLongVector) vector);
    }
  }

  public static void deserializeVector(ByteBuf buf, FloatVector vector) {
    if (vector instanceof IntFloatVector) {
      deserializeVector(buf, (IntFloatVector) vector);
    } else {
      deserializeVector(buf, (LongFloatVector) vector);
    }
  }

  public static void deserializeVector(ByteBuf buf, DoubleVector vector) {
    if (vector instanceof IntDoubleVector) {
      deserializeVector(buf, (IntDoubleVector) vector);
    } else {
      deserializeVector(buf, (LongDoubleVector) vector);
    }
  }

  public static void deserializeVector(ByteBuf buf, IntIntVector vector) {
    int elemNum = buf.readInt();
    StorageMethod method = getStorageMethod(vector);
    SerializeArrangement arrangement = SerializeArrangement.valuesOf(buf.readInt());

    if (arrangement == SerializeArrangement.KEY_VALUE) {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        int[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = buf.readInt();
          values[i] = buf.readInt();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(buf.readInt(), buf.readInt());
        }
      }
    } else {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        int[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = i;
          values[i] = buf.readInt();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(i, buf.readInt());
        }
      }
    }
  }

  public static void deserializeVector(ByteBuf buf, IntLongVector vector) {
    int elemNum = buf.readInt();
    StorageMethod method = getStorageMethod(vector);
    SerializeArrangement arrangement = SerializeArrangement.valuesOf(buf.readInt());

    if (arrangement == SerializeArrangement.KEY_VALUE) {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        int[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = buf.readInt();
          values[i] = buf.readLong();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(buf.readInt(), buf.readLong());
        }
      }
    } else {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        int[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = i;
          values[i] = buf.readLong();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(i, buf.readLong());
        }
      }
    }
  }

  public static void deserializeVector(ByteBuf buf, IntFloatVector vector) {
    int elemNum = buf.readInt();
    StorageMethod method = getStorageMethod(vector);
    SerializeArrangement arrangement = SerializeArrangement.valuesOf(buf.readInt());

    if (arrangement == SerializeArrangement.KEY_VALUE) {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        int[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = buf.readInt();
          values[i] = buf.readFloat();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(buf.readInt(), buf.readFloat());
        }
      }
    } else {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        int[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = i;
          values[i] = buf.readFloat();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(i, buf.readFloat());
        }
      }
    }
  }

  public static void deserializeVector(ByteBuf buf, IntDoubleVector vector) {
    int elemNum = buf.readInt();
    StorageMethod method = getStorageMethod(vector);
    SerializeArrangement arrangement = SerializeArrangement.valuesOf(buf.readInt());

    if (arrangement == SerializeArrangement.KEY_VALUE) {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        int[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = buf.readInt();
          values[i] = buf.readDouble();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(buf.readInt(), buf.readDouble());
        }
      }
    } else {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        int[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = i;
          values[i] = buf.readDouble();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(i, buf.readDouble());
        }
      }
    }
  }

  public static void deserializeVector(ByteBuf buf, LongIntVector vector) {
    int elemNum = buf.readInt();
    StorageMethod method = getStorageMethod(vector);
    SerializeArrangement arrangement = SerializeArrangement.valuesOf(buf.readInt());

    if (arrangement == SerializeArrangement.KEY_VALUE) {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        long[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = buf.readLong();
          values[i] = buf.readInt();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(buf.readLong(), buf.readInt());
        }
      }
    } else {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        long[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = i;
          values[i] = buf.readInt();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(i, buf.readInt());
        }
      }
    }
  }

  public static void deserializeVector(ByteBuf buf, LongLongVector vector) {
    int elemNum = buf.readInt();
    StorageMethod method = getStorageMethod(vector);
    SerializeArrangement arrangement = SerializeArrangement.valuesOf(buf.readInt());

    if (arrangement == SerializeArrangement.KEY_VALUE) {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        long[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = buf.readLong();
          values[i] = buf.readLong();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(buf.readLong(), buf.readLong());
        }
      }
    } else {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        long[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = i;
          values[i] = buf.readLong();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(i, buf.readLong());
        }
      }
    }
  }

  public static void deserializeVector(ByteBuf buf, LongFloatVector vector) {
    int elemNum = buf.readInt();
    StorageMethod method = getStorageMethod(vector);
    SerializeArrangement arrangement = SerializeArrangement.valuesOf(buf.readInt());

    if (arrangement == SerializeArrangement.KEY_VALUE) {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        long[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = buf.readLong();
          values[i] = buf.readFloat();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(buf.readLong(), buf.readFloat());
        }
      }
    } else {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        long[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = i;
          values[i] = buf.readFloat();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(i, buf.readFloat());
        }
      }
    }
  }

  public static void deserializeVector(ByteBuf buf, LongDoubleVector vector) {
    int elemNum = buf.readInt();
    StorageMethod method = getStorageMethod(vector);
    SerializeArrangement arrangement = SerializeArrangement.valuesOf(buf.readInt());

    if (arrangement == SerializeArrangement.KEY_VALUE) {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        long[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = buf.readLong();
          values[i] = buf.readDouble();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(buf.readLong(), buf.readDouble());
        }
      }
    } else {
      if (method == StorageMethod.SORTED) {
        // If use sorted storage, we should get the array pair first
        long[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        for (int i = 0; i < elemNum; i++) {
          indices[i] = i;
          values[i] = buf.readDouble();
        }
      } else {
        for (int i = 0; i < elemNum; i++) {
          vector.set(i, buf.readDouble());
        }
      }
    }
  }


  public static int bufferLen(Vector vector) {
    int headLen = 4 * 4 + 2 * 8 + 4 * 2;
    StorageMethod method = getStorageMethod(vector);
    BasicType keyType = getKeyType(vector);
    BasicType valueType = getValueType(vector);
    int len = (int) vector.getSize();

    if (method == StorageMethod.DENSE) {
      return headLen + len * sizeOf(valueType);
    } else {
      return headLen + len * (sizeOf(keyType) + sizeOf(valueType));
    }
  }

  public static int sizeOf(BasicType type) {
    if (type == BasicType.INT || type == BasicType.FLOAT) {
      return 4;
    } else {
      return 8;
    }
  }

  public static void clear(Vector vector) {
    vector.clear();
  }

  public static int size(Vector vector) {
    return (int) vector.getSize();
  }
}
