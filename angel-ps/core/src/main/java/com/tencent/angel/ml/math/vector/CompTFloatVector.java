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

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.executor.MatrixOpExecutors;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

/**
 * Base class of component double vector.
 */
abstract class CompTFloatVector extends TIntFloatVector {
  private static final Log LOG = LogFactory.getLog(CompTFloatVector.class);

  /**
   * The splits of the row, they are sorted by the start column index
   */
  protected final TFloatVector[] vectors;

  /**
   * The Partitions that contain this row, they are sorted by the start column index
   */
  protected final PartitionKey[] partKeys;

  /**
   * The number of splits
   */
  protected final int splitNum;

  /**
   * The column number in a split
   */
  protected final int splitLen;

  /**
   * The estimate capacity of a split
   */
  protected final int initCapacity;

  /**
   * Create a CompTFloatVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim      vector dimension
   * @param nnz      element number of the vector
   */
  public CompTFloatVector(int matrixId, int rowIndex, int dim, int nnz) {
    super();
    setMatrixId(matrixId);
    setRowId(rowIndex);
    this.dim = dim;

    List<PartitionKey> partKeyList =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowIndex);

    if (partKeyList.size() >= 1) {
      Collections.sort(partKeyList, new Comparator<PartitionKey>() {
        @Override public int compare(PartitionKey key1, PartitionKey key2) {
          return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
        }
      });
    }

    partKeys = partKeyList.toArray(new PartitionKey[0]);
    splitNum = partKeys.length;
    vectors = new TFloatVector[splitNum];
    if (splitNum > 0) {
      splitLen = (int) (partKeys[0].getEndCol() - partKeys[0].getStartCol());
      if (nnz > 0) {
        initCapacity = (int) (nnz / splitNum);
      } else {
        initCapacity = -1;
      }
    } else {
      splitLen = 0;
      initCapacity = -1;
    }
  }

  /**
   * Create a CompTFloatVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim      vector dimension
   * @param partKeys the partitions that contains this vector
   * @param splits   vector splits
   */
  public CompTFloatVector(int matrixId, int rowIndex, int dim, PartitionKey[] partKeys,
    TFloatVector[] splits) {
    super();
    setMatrixId(matrixId);
    setRowId(rowIndex);
    this.dim = dim;

    assert partKeys.length == splits.length;
    this.vectors = splits;
    this.partKeys = partKeys;
    splitNum = splits.length;

    if (splitNum > 0) {
      splitLen = (int) (partKeys[0].getEndCol() - partKeys[0].getStartCol());
      initCapacity = splits[0].size();
    } else {
      splitLen = 0;
      initCapacity = -1;
    }
  }

  static class TimesByOp extends RecursiveAction {
    private final TFloatVector[] rowSplits;
    private final float factor;
    private final int startPos;
    private final int endPos;

    public TimesByOp(TFloatVector[] rowSplits, int startPos, int endPos, float factor) {
      this.rowSplits = rowSplits;
      this.startPos = startPos;
      this.endPos = endPos;
      this.factor = factor;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        if (rowSplits[startPos] == null) {
          return;
        } else {
          rowSplits[startPos].timesBy(factor);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        TimesByOp opLeft = new TimesByOp(rowSplits, startPos, middle, factor);
        TimesByOp opRight = new TimesByOp(rowSplits, middle, endPos, factor);
        invokeAll(opLeft, opRight);
      }
    }
  }


  class PlusByOp extends RecursiveAction {
    private final TFloatVector[] leftSplits;
    private final TFloatVector[] rightSplits;
    private final int startPos;
    private final int endPos;

    public PlusByOp(TFloatVector[] leftSplits, TFloatVector[] rightSplits, int startPos,
      int endPos) {
      this.leftSplits = leftSplits;
      this.rightSplits = rightSplits;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        if (leftSplits[startPos] != null && rightSplits[startPos] != null) {
          leftSplits[startPos].plusBy(rightSplits[startPos]);
        } else if (leftSplits[startPos] == null && rightSplits[startPos] != null) {
          leftSplits[startPos] = initComponentVector(rightSplits[startPos]);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        PlusByOp opLeft = new PlusByOp(leftSplits, rightSplits, startPos, middle);
        PlusByOp opRight = new PlusByOp(leftSplits, rightSplits, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }


  class PlusByWithFactorOp extends RecursiveAction {
    private final TFloatVector[] leftSplits;
    private final TFloatVector[] rightSplits;
    private final int startPos;
    private final int endPos;
    private final float factor;

    public PlusByWithFactorOp(TFloatVector[] leftSplits, TFloatVector[] rightSplits, int startPos,
      int endPos, float factor) {
      this.leftSplits = leftSplits;
      this.rightSplits = rightSplits;
      this.startPos = startPos;
      this.endPos = endPos;
      this.factor = factor;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        if (leftSplits[startPos] != null && rightSplits[startPos] != null) {
          leftSplits[startPos].plusBy(rightSplits[startPos], factor);
        } else if (leftSplits[startPos] == null && rightSplits[startPos] != null) {
          leftSplits[startPos] = initComponentVector(rightSplits[startPos]);
          leftSplits[startPos].timesBy(factor);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        PlusByWithFactorOp opLeft =
          new PlusByWithFactorOp(leftSplits, rightSplits, startPos, middle, factor);
        PlusByWithFactorOp opRight =
          new PlusByWithFactorOp(leftSplits, rightSplits, middle, endPos, factor);
        invokeAll(opLeft, opRight);
      }
    }
  }


  class DotOp extends RecursiveTask<Double> {
    private final TFloatVector[] leftSplits;
    private final TFloatVector[] rightSplits;
    private final int startPos;
    private final int endPos;

    public DotOp(TFloatVector[] leftSplits, TFloatVector[] rightSplits, int startPos, int endPos) {
      this.leftSplits = leftSplits;
      this.rightSplits = rightSplits;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected Double compute() {
      if (endPos <= startPos) {
        return 0.0;
      }

      if (endPos - startPos == 1) {
        if (leftSplits[startPos] != null && rightSplits[startPos] != null) {
          return leftSplits[startPos].dot(rightSplits[startPos]);
        } else {
          return 0.0;
        }
      } else {
        int middle = (startPos + endPos) / 2;
        DotOp opLeft = new DotOp(leftSplits, rightSplits, startPos, middle);
        DotOp opRight = new DotOp(leftSplits, rightSplits, middle, endPos);
        invokeAll(opLeft, opRight);

        try {
          return opLeft.get() + opRight.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("DosOp failed " + e.getMessage());
          return 0.0;
        }
      }
    }
  }


  class NNZCounterOp extends RecursiveTask<Long> {
    private final TFloatVector[] splits;
    private final int startPos;
    private final int endPos;

    public NNZCounterOp(TFloatVector[] splits, int startPos, int endPos) {
      this.splits = splits;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected Long compute() {
      if (endPos <= startPos) {
        return 0L;
      }

      if (endPos - startPos == 1) {
        if (splits[startPos] != null) {
          return splits[startPos].nonZeroNumber();
        } else {
          return 0L;
        }
      } else {
        int middle = (startPos + endPos) / 2;
        NNZCounterOp opLeft = new NNZCounterOp(splits, startPos, middle);
        NNZCounterOp opRight = new NNZCounterOp(splits, middle, endPos);
        invokeAll(opLeft, opRight);

        try {
          return opLeft.get() + opRight.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("NNZCounterOp failed " + e.getMessage());
          return 0L;
        }
      }
    }
  }


  class SquaredNormOp extends RecursiveTask<Double> {
    private final TFloatVector[] splits;
    private final int startPos;
    private final int endPos;

    public SquaredNormOp(TFloatVector[] splits, int startPos, int endPos) {
      this.splits = splits;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected Double compute() {
      if (endPos <= startPos) {
        return 0.0;
      }
      if (endPos - startPos == 1) {
        if (splits[startPos] != null) {
          return splits[startPos].squaredNorm();
        } else {
          return 0.0;
        }
      } else {
        int middle = (startPos + endPos) / 2;
        SquaredNormOp opLeft = new SquaredNormOp(splits, startPos, middle);
        SquaredNormOp opRight = new SquaredNormOp(splits, middle, endPos);
        invokeAll(opLeft, opRight);

        try {
          return opLeft.get() + opRight.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("NNZCounterOp failed " + e.getMessage());
          return 0.0;
        }
      }
    }
  }


  class SumOp extends RecursiveTask<Double> {
    private final TFloatVector[] splits;
    private final int startPos;
    private final int endPos;

    public SumOp(TFloatVector[] splits, int startPos, int endPos) {
      this.splits = splits;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected Double compute() {
      if (endPos <= startPos) {
        return 0.0;
      }

      if (endPos - startPos == 1) {
        if (splits[startPos] != null) {
          return splits[startPos].sum();
        } else {
          return 0.0;
        }
      } else {
        int middle = (startPos + endPos) / 2;
        SumOp opLeft = new SumOp(splits, startPos, middle);
        SumOp opRight = new SumOp(splits, middle, endPos);
        invokeAll(opLeft, opRight);

        try {
          return opLeft.get() + opRight.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("NNZCounterOp failed " + e.getMessage());
          return 0.0;
        }
      }
    }
  }

  /**
   * Init a split vector
   *
   * @return split vector
   */
  protected abstract TFloatVector initComponentVector();

  /**
   * Init a split vector
   *
   * @param initCapacity the initCapacity for split vector
   * @return split vector
   */
  protected abstract TFloatVector initComponentVector(int initCapacity);

  /**
   * Init a split vector from other vector
   *
   * @param vector a vector that belongs the same partition
   * @return split vector
   */
  protected abstract TFloatVector initComponentVector(TFloatVector vector);

  @Override public int[] getIndices() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public float[] getValues() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public float get(int index) {
    int partIndex = (int) (index / splitLen);
    if (vectors[partIndex] == null) {
      return 0.0f;
    } else {
      return vectors[partIndex].get(index);
    }
  }

  @Override public TFloatVector set(int index, float value) {
    int partIndex = (int) (index / splitLen);
    if (vectors[partIndex] == null) {
      vectors[partIndex] = initComponentVector();
    }
    vectors[partIndex].set(index, value);

    return this;
  }

  @Override public TFloatVector plusBy(int index, float delta) {
    int partIndex = (int) (index / splitLen);
    if (vectors[partIndex] == null) {
      vectors[partIndex] = initComponentVector();
    }
    vectors[partIndex].plusBy(index, delta);
    return this;
  }

  @Override public double sum() {
    SumOp op = new SumOp(vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    return op.join();
  }


  @Override public TFloatVector plusBy(TAbstractVector other, float x) {
    if (other instanceof CompTFloatVector) {
      return plusBy((CompTFloatVector) other, x);
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private TFloatVector plusBy(CompTFloatVector other, float x) {
    PlusByWithFactorOp op = new PlusByWithFactorOp(vectors, other.vectors, 0, splitNum, x);
    MatrixOpExecutors.execute(op);
    op.join();
    return this;
  }

  @Override public TVector plusBy(TAbstractVector other) {
    if (other instanceof CompTFloatVector) {
      return plusBy((CompTFloatVector) other);
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private TVector plusBy(CompTFloatVector other) {
    PlusByOp op = new PlusByOp(vectors, other.vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    op.join();
    return this;
  }

  @Override public TFloatVector filter(float x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TFloatVector times(float x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TFloatVector timesBy(float x) {
    TimesByOp op = new TimesByOp(vectors, 0, splitNum, x);
    MatrixOpExecutors.execute(op);
    op.join();
    return this;
  }

  @Override public TVector plus(TAbstractVector other, float x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector plus(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public double dot(TAbstractVector other) {
    if (other instanceof CompTFloatVector) {
      return dot((CompTFloatVector) other);
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(CompTFloatVector other) {
    DotOp op = new DotOp(vectors, other.vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    return op.join();
  }

  @Override public void clear() {
    for (int i = 0; i < splitNum; i++) {
      vectors[i] = null;
    }
  }

  @Override public long nonZeroNumber() {
    NNZCounterOp op = new NNZCounterOp(vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    return op.join();
  }

  @Override public double squaredNorm() {
    SquaredNormOp op = new SquaredNormOp(vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    return op.join();
  }

  @Override public double sparsity() {
    return nonZeroNumber() / getDimension();
  }

  @Override public int size() {
    int size = 0;
    for (int i = 0; i < splitNum; i++) {
      if (vectors[i] != null) {
        size += vectors[i].size();
      }
    }
    return size;
  }

  /**
   * Get the splits
   *
   * @return the splits
   */
  public PartitionKey[] getPartKeys() {
    return partKeys;
  }

  /**
   * Get the partitions
   *
   * @return the partitions
   */
  public TFloatVector[] getVectors() {
    return vectors;
  }
}
