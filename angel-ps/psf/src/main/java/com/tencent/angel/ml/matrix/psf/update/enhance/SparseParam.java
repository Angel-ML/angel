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
 */

package com.tencent.angel.ml.matrix.psf.update.enhance;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SparseParam extends UpdateParam {

  private Log LOG = LogFactory.getLog(SparseParam.class);

  public static class SparseFPartitionUpdateParam extends PartitionUpdateParam {
    private int rowId;
    private long[] cols;
    private double[] values;

    public SparseFPartitionUpdateParam(int matrixId, PartitionKey partKey,
                                       int rowId, long[] cols, double[] values) {
      super(matrixId, partKey, false);
      assert (cols.length == values.length);
      this.rowId = rowId;
      this.cols = cols;
      this.values = values;
    }

    public int getRowId() {
      return rowId;
    }

    public long[] getColIds() {
      return cols;
    }

    public double[] getValues() {
      return values;
    }

    public SparseFPartitionUpdateParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);

      buf.writeInt(cols.length);
      for (long value: cols) {
        buf.writeLong(value);
      }

      buf.writeInt(values.length);
      for (double value: values) {
        buf.writeDouble(value);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
     this.rowId = buf.readInt();

      int size = buf.readInt();
      long[] longs = new long[size];
      for (int i = 0; i < size; i++) {
        longs[i] = buf.readLong();
      }
      this.cols = longs;

      size = buf.readInt();
      double[] doubles = new double[size];
      for (int i = 0; i < size; i++) {
        doubles[i] = buf.readDouble();
      }
      this.values = doubles;
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 4 + (4 + cols.length * 8) + (4 + values.length * 8);
    }
  }


  private int rowId;
  private long[] cols;
  private double[] values;

  public SparseParam(int matrixId) {
    super(matrixId, false);
  }

  public SparseParam(int matrixId, int rowId, long[] cols, double[] values) {
    super(matrixId, false);
    assert(cols.length == values.length);
    this.rowId = rowId;
    Arrays.parallelQuickSort(0, cols.length, new IndexComp(cols), new IndexValueSwapper(cols, values));
    this.cols = cols;
    this.values = values;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> parts = PSAgentContext.get()
        .getMatrixMetaManager()
        .getPartitions(matrixId);

    int beginIndex = 0;
    int endIndex = 0;
    List<PartitionUpdateParam> partParams = new ArrayList<>();

    for (PartitionKey part: parts) {
      if (Utils.withinPart(part, new int[]{rowId})) {
        if (beginIndex < cols.length && cols[beginIndex] >= part.getStartCol()) {
          while(endIndex < cols.length && cols[endIndex] < part.getEndCol()) endIndex++;

          long[] thisCols = new long[endIndex - beginIndex];
          double[] thisValues = new double[endIndex - beginIndex];
          System.arraycopy(cols, beginIndex, thisCols, 0, endIndex - beginIndex);
          System.arraycopy(values, beginIndex, thisValues, 0, endIndex - beginIndex);
          SparseFPartitionUpdateParam partParam =
              new SparseFPartitionUpdateParam(matrixId, part, rowId, thisCols, thisValues);

          partParams.add(partParam);
          if (endIndex == cols.length) break;
          beginIndex = endIndex;
        }
      }
    }

    return partParams;
  }
}

class IndexComp implements IntComparator {

  private long[] indices;
  public IndexComp(long[] indices) {
    this.indices = indices;
  }

  @Override
  public int compare(int k1, int k2) {
    if (indices[k1] < indices[k2]) {
      return -1;
    } else if (indices[k1] > indices[k2]) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public int compare(Integer k1, Integer k2) {
    if (indices[k1] < indices[k2]) {
      return -1;
    } else if (indices[k1] > indices[k2]) {
      return 1;
    } else {
      return 0;
    }
  }
}


class IndexValueSwapper implements Swapper {

  private long[] indices;
  private double[] values;
  public IndexValueSwapper(long[] index, double[] value) {
    this.indices = index;
    this.values = value;
  }

  @Override
  public void swap(int a, int b) {
    long tempIndex = indices[a];
    indices[a] = indices[b];
    indices[b] = tempIndex;

    double tempValue = values[a];
    values[a] = values[b];
    values[b] = tempValue;
  }
}