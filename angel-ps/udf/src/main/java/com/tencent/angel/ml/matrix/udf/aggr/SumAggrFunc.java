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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.matrix.udf.aggr;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerDenseFloatRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseFloatRow;
import com.tencent.angel.psagent.PSAgentContext;

/**
 * The function of summary.
 */
public class SumAggrFunc extends DefaultAggrFunc {
  /**
   * Creates a new summary function.
   *
   * @param param the param
   */
  public SumAggrFunc(SumAggrParam param) {
    super(param);
  }

  /**
   * Creates a new summary function by default.
   */
  public SumAggrFunc(){
    super(null);
  }

  /**
   * The summary parameter.
   */
  public static class SumAggrParam extends AggrParam {
    /**
     * Creates a new summary parameter.
     *
     * @param matrixId the matrix id
     */
    public SumAggrParam(int matrixId) {
      super(matrixId);
    }
  }

  /**
   * The result of partition summary.
   */
  public static class SumPartitionAggrResult extends PartitionAggrResult {
    private double result;

    /**
     * Creates a new summary partition result.
     *
     * @param result the result
     */
    public SumPartitionAggrResult(double result) {
      this.result = result;
    }

    /**
     * Creates a new summary partition result by default.
     */
    public SumPartitionAggrResult() {
      result = 0.0;
    }

    @Override
    public void serialize(ByteBuf buf) {
      buf.writeDouble(result);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      result = buf.readDouble();
    }

    @Override
    public int bufferLen() {
      return 8;
    }
  }

  /**
   * The result of summary.
   */
  public static class SumAggrResult extends AggrResult {
    private final double result;

    /**
     * Creates a result.
     *
     * @param result the result
     */
    public SumAggrResult(double result) {
      this.result = result;
    }

    /**
     * Gets result.
     *
     * @return the result
     */
    public double getResult() {
      return result;
    }
  }

  @Override
  public PartitionAggrResult aggr(PartitionAggrParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    double sum = 0.0;
    if (part != null) {
      int startRow = part.getPartitionKey().getStartRow();
      int endRow = part.getPartitionKey().getEndRow();
      for (int i = startRow; i < endRow; i++) {
        ServerRow row = part.getRow(i);
        sum += sum(row);
      }
    }

    return new SumPartitionAggrResult(sum);
  }

  private double sum(ServerRow row) {
    switch (row.getRowType()) {
      case T_DOUBLE_SPARSE:
        return sum((ServerSparseDoubleRow) row);

      case T_DOUBLE_DENSE:
        return sum((ServerDenseDoubleRow) row);

      case T_INT_SPARSE:
        return sum((ServerSparseIntRow) row);

      case T_INT_DENSE:
        return sum((ServerDenseIntRow) row);

      case T_FLOAT_SPARSE:
        return sum((ServerSparseFloatRow) row);

      case T_FLOAT_DENSE:
        return sum((ServerDenseFloatRow) row);

      default:
        return 0.0;
    }
  }

  private double sum(ServerSparseDoubleRow row) {
    double sum = 0.0;
    try {
      row.getLock().readLock().lock();
      ObjectIterator<it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry> iter =
          row.getData().int2DoubleEntrySet().fastIterator();
      while (iter.hasNext()) {
        sum += iter.next().getDoubleValue();
      }

      return sum;
    } finally {
      row.getLock().readLock().unlock();
    }
  }

  private double sum(ServerDenseDoubleRow row) {
    double sum = 0.0;
    try {
      row.getLock().readLock().lock();
      DoubleBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        sum += data.get(i);
      }

      return sum;
    } finally {
      row.getLock().readLock().unlock();
    }
  }

  private double sum(ServerSparseIntRow row) {
    double sum = 0.0;
    try {
      row.getLock().readLock().lock();
      ObjectIterator<it.unimi.dsi.fastutil.ints.Int2IntMap.Entry> iter =
          row.getData().int2IntEntrySet().fastIterator();
      while (iter.hasNext()) {
        sum += iter.next().getIntValue();
      }

      return sum;
    } finally {
      row.getLock().readLock().unlock();
    }
  }

  private double sum(ServerDenseIntRow row) {
    double sum = 0.0;
    try {
      row.getLock().readLock().lock();
      IntBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        sum += data.get(i);
      }

      return sum;
    } finally {
      row.getLock().readLock().unlock();
    }
  }

  private double sum(ServerSparseFloatRow row) {
    double sum = 0.0;
    try {
      row.getLock().readLock().lock();
      ObjectIterator<it.unimi.dsi.fastutil.ints.Int2FloatMap.Entry> iter =
          row.getData().int2FloatEntrySet().fastIterator();
      while (iter.hasNext()) {
        sum += iter.next().getFloatValue();
      }

      return sum;
    } finally {
      row.getLock().readLock().unlock();
    }
  }

  private double sum(ServerDenseFloatRow row) {
    double sum = 0.0;
    try {
      row.getLock().readLock().lock();
      FloatBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        sum += data.get(i);
      }

      return sum;
    } finally {
      row.getLock().readLock().unlock();
    }
  }

  @Override
  public AggrResult merge(List<PartitionAggrResult> partResults) {
    double sum = 0.0;
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      sum += ((SumPartitionAggrResult) partResults.get(i)).result;
    }

    return new SumAggrResult(sum);
  }
}
