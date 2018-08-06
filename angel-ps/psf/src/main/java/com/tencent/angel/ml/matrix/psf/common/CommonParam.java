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

package com.tencent.angel.ml.matrix.psf.common;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class CommonParam extends UpdateParam {

  public static class PSFPartitionUpdateParam extends PartitionUpdateParam {
    private int[] intArray;
    private long[] longArray;
    private float[] floatArray;
    private double[] doubleArray;

    public PSFPartitionUpdateParam(int matrixId, PartitionKey partKey,
                                   int[] ints, long[] longs, float[] floats, double[] doubles) {
      super(matrixId, partKey, false);
      intArray = ints;
      longArray = longs;
      floatArray = floats;
      doubleArray = doubles;
    }

    public int[] getInts() {
      return intArray;
    }

    public long[] getLongs() {
      return longArray;
    }

    public float[] getFloats() {
      return floatArray;
    }

    public double[] getDoubles() {
      return doubleArray;
    }

    public PSFPartitionUpdateParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(intArray.length);
      for (int value: intArray) {
        buf.writeInt(value);
      }
      buf.writeInt(longArray.length);
      for (long value: longArray) {
        buf.writeLong(value);
      }
      buf.writeInt(floatArray.length);
      for (float value: floatArray) {
        buf.writeFloat(value);
      }
      buf.writeInt(doubleArray.length);
      for (double value: doubleArray) {
        buf.writeDouble(value);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      int size = buf.readInt();
      int[] ints = new int[size];
      for (int i = 0; i < size; i++) {
        ints[i] = buf.readInt();
      }
      this.intArray = ints;

      size = buf.readInt();
      long[] longs = new long[size];
      for (int i = 0; i < size; i++) {
        longs[i] = buf.readLong();
      }
      this.longArray = longs;

      size = buf.readInt();
      float[] floats = new float[size];
      for (int i = 0; i < size; i++) {
        floats[i] = buf.readFloat();
      }
      this.floatArray = floats;

      size = buf.readInt();
      double[] doubles = new double[size];
      for (int i = 0; i < size; i++) {
        doubles[i] = buf.readDouble();
      }
      this.doubleArray = doubles;
    }

  }


  private ArrayList<Integer> intArray = new ArrayList<>();
  private ArrayList<Long> longArray = new ArrayList<>();
  private ArrayList<Float> floatArray = new ArrayList<>();
  private ArrayList<Double> doubleArray = new ArrayList<>();

  public CommonParam(int matrixId) {
    super(matrixId, false);
  }

  public CommonParam(int matrixId, int[] ints) {
    this(matrixId, ints, new long[]{}, new double[]{});
  }

  public CommonParam(int matrixId, int[] ints, double[] doubles) {
    this(matrixId, ints, new long[]{}, doubles);
  }

  public CommonParam(int matrixId, long[] longs, double[] doubles) {
    this(matrixId, new int[]{}, longs, doubles);
  }

  public CommonParam(int matrixId, int[] ints, long[] longs, double[] doubles) {
    super(matrixId, false);
    for (int i : ints) setInt(i);
    for (long l : longs) setLong(l);
    for (double d : doubles) setDouble(d);
  }

  public void setInt(int i) {
    intArray.add(i);
  }

  public void setLong(long l) {
    longArray.add(l);
  }

  public void setFloat(float f) {
    floatArray.add(f);
  }

  public void setDouble(Double d) {
    doubleArray.add(d);
  }


  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> parts = PSAgentContext.get()
        .getMatrixMetaManager()
        .getPartitions(matrixId);

    int size = parts.size();
    List<PartitionUpdateParam> partParams = new ArrayList<PartitionUpdateParam>(size);
    for (PartitionKey part : parts) {
      int[] ints = Utils.intListToArray(intArray);
      long[] longs = Utils.longListToArray(longArray);
      float[] floats = Utils.floatListToArray(floatArray);
      double[] doubles = Utils.doubleListToArray(doubleArray);
      PSFPartitionUpdateParam partParam = new PSFPartitionUpdateParam(matrixId, part, ints, longs, floats, doubles);
      partParams.add(partParam);
    }

    return partParams;
  }
}
