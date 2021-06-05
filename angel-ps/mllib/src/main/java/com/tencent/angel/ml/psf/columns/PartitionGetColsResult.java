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


package com.tencent.angel.ml.psf.columns;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PartitionGetColsResult extends PartitionGetResult {
  private static Log LOG = LogFactory.getLog(PartitionGetColsResult.class);

  int[] rows;
  long[] cols;
  Vector vector;

  public PartitionGetColsResult(int[] rows, long[] cols, Vector vector) {
    this.rows = rows;
    this.cols = cols;
    this.vector = vector;
  }

  public PartitionGetColsResult() {
  }

  @Override public void serialize(ByteBuf buf) {
    serialize(buf, rows, cols, vector);
  }

  private static void serialize(ByteBuf buf, CompIntDoubleVector vector, long[] cols) {
    buf.writeByte(0);
    IntDoubleVector[] parts = vector.getPartitions();
    for (int c = 0; c < cols.length; c++) {
      buf.writeLong(cols[c]);
      double[] values = parts[c].getStorage().getValues();
      for (int r = 0; r < values.length; r++)
        buf.writeDouble(values[r]);
    }
  }

  private static void serialize(ByteBuf buf, CompIntFloatVector vector, long[] cols) {
    buf.writeByte(1);
    IntFloatVector[] parts = vector.getPartitions();

    for (int c = 0; c < cols.length; c++) {
      buf.writeLong(cols[c]);
      float[] values = parts[c].getStorage().getValues();
      for (int r = 0; r < values.length; r++)
        buf.writeFloat(values[r]);
    }
  }

  public static void serialize(ByteBuf buf, int[] rows, long[] cols, Vector vector) {
    buf.writeInt(rows.length);
    for (int i = 0; i < rows.length; i++)
      buf.writeInt(rows[i]);
    buf.writeInt(cols.length);
    serialize(buf, cols, vector);
  }

  public static void serialize(ByteBuf buf, long[] cols, Vector vector) {
    if (vector instanceof CompIntDoubleVector) {
      serialize(buf, (CompIntDoubleVector) vector, cols);
    } else if (vector instanceof CompIntFloatVector) {
      serialize(buf, (CompIntFloatVector) vector, cols);
    } else {
      throw new AngelException("Data Type should be double or float!");
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    int nRows = buf.readInt();
    rows = new int[nRows];
    for (int i = 0; i < nRows; i++)
      rows[i] = buf.readInt();
    int nCols = buf.readInt();
    //    System.out.println("deserialize cols.length=" + nCols);
    cols = new long[nCols];
    vector = deserialize(buf, rows, cols);
  }

  public static Vector deserialize(ByteBuf buf, int[] rows, long[] cols) {
    switch (buf.readByte()) {
      case 0:
        return deserialize(buf, new IntDoubleVector[cols.length], rows.length, cols);
      case 1:
        return deserialize(buf, new IntFloatVector[cols.length], rows.length, cols);
      default:
        throw new AngelException("Data Type should be double or float!");
    }
  }

  private static CompIntDoubleVector deserialize(ByteBuf buf, IntDoubleVector[] vectors, int subDim,
    long[] cols) {
    for (int c = 0; c < cols.length; c++) {
      cols[c] = buf.readLong();
      double[] values = new double[subDim];
      for (int r = 0; r < subDim; r++)
        values[r] = buf.readDouble();
      vectors[c] = VFactory.denseDoubleVector(values);
    }
    return VFactory.compIntDoubleVector(subDim * cols.length, vectors, subDim);
  }

  private static CompIntFloatVector deserialize(ByteBuf buf, IntFloatVector[] vectors, int subDim,
    long[] cols) {
    //    System.out.print("deserialize ");
    for (int c = 0; c < cols.length; c++) {
      cols[c] = buf.readLong();
      //      System.out.print(cols[c] + " ");
      float[] values = new float[subDim];
      for (int r = 0; r < subDim; r++)
        values[r] = buf.readFloat();
      vectors[c] = VFactory.denseFloatVector(values);
    }
    //    System.out.println();
    return VFactory.compIntFloatVector(subDim * cols.length, vectors, subDim);
  }

  @Override public int bufferLen() {
    int len = 8 + rows.length * 4;
    if (vector instanceof CompIntDoubleVector) {
      len += 1 + cols.length * 8 + rows.length * cols.length * 8;
    } else if (vector instanceof CompIntFloatVector) {
      len += 1 + cols.length * 8 + rows.length * cols.length * 4;
    } else {
      return len;
    }

    return len;
  }

  public static int bufferLen(int[] rows, long[] cols, Vector vector) {
    int len = 8 + rows.length * 4;
    if (vector instanceof CompIntDoubleVector) {
      len += 1 + cols.length * 8 + rows.length * cols.length * 8;
    } else if (vector instanceof CompIntFloatVector) {
      len += 1 + cols.length * 8 + rows.length * cols.length * 4;
    } else {
      return len;
    }

    return len;
  }
}
