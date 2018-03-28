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
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

/**
 * Base class of component double vector with long key.
 */
public abstract class CompLongKeyDoubleVector extends TLongDoubleVector {
  private static final Log LOG = LogFactory.getLog(SparseLongKeyDoubleVector.class);

  /** The splits of the row, they are sorted by the start column index*/
  protected final TLongDoubleVector[] vectors;

  /** The Partitions that contain this row, they are sorted by the start column index */
  protected final PartitionKey[] partKeys;

  /** The number of splits */
  protected final int splitNum;

  /** The column number in a split */
  protected final long splitLen;

  /** The estimate capacity of a split */
  protected final int initCapacity;

  /** Make up position is need if dim <= 0(vector range is [Long.MIN_VALUE, Long.MAX_VALUE]) */
  private final long makeup;
  private final int makeupPos;

  class TimesByOp extends RecursiveAction {
    private final TLongDoubleVector[] rowSplits;
    private final double factor;
    private final int startPos;
    private final int endPos;

    public TimesByOp(TLongDoubleVector[] rowSplits, int startPos, int endPos,
                     double factor) {
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
    private final TLongDoubleVector[] leftSplits;
    private final TLongDoubleVector[] rightSplits;
    private final int startPos;
    private final int endPos;

    public PlusByOp(TLongDoubleVector[] leftSplits, TLongDoubleVector[] rightSplits,
                    int startPos, int endPos) {
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
    private final TLongDoubleVector[] leftSplits;
    private final TLongDoubleVector[] rightSplits;
    private final int startPos;
    private final int endPos;
    private final double factor;

    public PlusByWithFactorOp(TLongDoubleVector[] leftSplits,
                              TLongDoubleVector[] rightSplits, int startPos, int endPos, double factor) {
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
    private final TLongDoubleVector[] leftSplits;
    private final TLongDoubleVector[] rightSplits;
    private final int startPos;
    private final int endPos;

    public DotOp(TLongDoubleVector[] leftSplits, TLongDoubleVector[] rightSplits,
                 int startPos, int endPos) {
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
    private final TLongDoubleVector[] splits;
    private final int startPos;
    private final int endPos;

    public NNZCounterOp(TLongDoubleVector[] splits, int startPos, int endPos) {
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
    private final TLongDoubleVector[] splits;
    private final int startPos;
    private final int endPos;

    public SquaredNormOp(TLongDoubleVector[] splits, int startPos, int endPos) {
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

  class NormOp extends RecursiveTask<Double> {
    private final TLongDoubleVector[] splits;
    private final int startPos;
    private final int endPos;

    public NormOp(TLongDoubleVector[] splits, int startPos, int endPos) {
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
          return splits[startPos].norm();
        } else {
          return 0.0;
        }
      } else {
        int middle = (startPos + endPos) / 2;
        NormOp opLeft = new NormOp(splits, startPos, middle);
        NormOp opRight = new NormOp(splits, middle, endPos);
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
    private final TLongDoubleVector[] splits;
    private final int startPos;
    private final int endPos;

    public SumOp(TLongDoubleVector[] splits, int startPos, int endPos) {
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

  class ElementUpdateOp extends RecursiveAction {
    private final TLongDoubleVector[] splits;
    private final int startPos;
    private final int endPos;
    private final LongDoubleElemUpdater updater;
    private final ElemUpdateParam param;

    public ElementUpdateOp(TLongDoubleVector[] splits, int startPos, int endPos, LongDoubleElemUpdater updater, ElemUpdateParam param) {
      this.splits = splits;
      this.startPos = startPos;
      this.endPos = endPos;
      this.updater = updater;
      this.param = param;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        if(splits[startPos] != null) {
          splits[startPos].elemUpdate(updater, param);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        ElementUpdateOp opLeft =
          new ElementUpdateOp(splits, startPos, middle, updater, param);
        ElementUpdateOp opRight =
          new ElementUpdateOp(splits, middle, endPos, updater, param);
        invokeAll(opLeft, opRight);
      }
    }
  }

  /**
   * Init a split vector
   *
   * @return split vector
   */
  protected abstract TLongDoubleVector initComponentVector();

  /**
   * Init a split vector
   *
   * @param initCapacity the initCapacity for split vector
   * @return split vector
   */
  protected abstract TLongDoubleVector initComponentVector(int initCapacity);

  /**
   * Init a split vector from other vector
   *
   * @param vector a vector that belongs the same partition
   * @return split vector
   */
  protected abstract TLongDoubleVector initComponentVector(TLongDoubleVector vector);

  /**
   * Create a CompSparseLongKeyDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param partKeys the partitions that contains this vector
   * @param splits vector splits
   */
  public CompLongKeyDoubleVector(int matrixId, int rowIndex, long dim,
                                 PartitionKey[] partKeys, TLongDoubleVector[] splits) {
    super(dim);
    setMatrixId(matrixId);
    setRowId(rowIndex);

    assert partKeys.length == splits.length;
    this.vectors = splits;
    this.partKeys = partKeys;
    splitNum = splits.length;

    if (splitNum > 0) {
      splitLen = partKeys[0].getEndCol() - partKeys[0].getStartCol();
      initCapacity = splits[0].size();
    } else {
      splitLen = 0;
      initCapacity = -1;
    }

    if(dim <= 0) {
      makeupPos = (int) (Long.MAX_VALUE / splitLen);
      makeup = Long.MAX_VALUE - makeupPos * splitLen;
    } else {
      makeupPos = 0;
      makeup = 0;
    }
  }

  /**
   * Create a CompSparseLongKeyDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public CompLongKeyDoubleVector(int matrixId, int rowIndex) {
    this(matrixId, rowIndex, -1, -1);
  }

  /**
   * Create a CompSparseLongKeyDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   */
  public CompLongKeyDoubleVector(int matrixId, int rowIndex, long dim) {
    this(matrixId, rowIndex, dim, -1);
  }

  /**
   *
   * Create a CompSparseLongKeyDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param nnz element number of the vector
   */
  public CompLongKeyDoubleVector(int matrixId, int rowIndex, long dim, long nnz) {
    super(dim);
    setMatrixId(matrixId);
    setRowId(rowIndex);

    List<PartitionKey> partKeyList =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowIndex);
    LOG.info("matrixId=" + matrixId + ", rowIndex=" + rowIndex + ", partNum=" + partKeyList.size());

    if (partKeyList.size() >= 1) {
      Collections.sort(partKeyList, new Comparator<PartitionKey>() {
        @Override public int compare(PartitionKey key1, PartitionKey key2) {
          return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
        }
      });
    }

    partKeys = partKeyList.toArray(new PartitionKey[0]);
    splitNum = partKeys.length;
    vectors = new TLongDoubleVector[splitNum];
    if (splitNum > 0) {
      splitLen = partKeys[0].getEndCol() - partKeys[0].getStartCol();
      if (nnz > 0) {
        initCapacity = (int) (nnz / splitNum);
      } else {
        initCapacity = -1;
      }
    } else {
      splitLen = 0;
      initCapacity = -1;
    }

    if(dim <= 0) {
      makeupPos = (int) (Long.MAX_VALUE / splitLen);
      makeup = Long.MAX_VALUE - makeupPos * splitLen;
    } else {
      makeupPos = 0;
      makeup = 0;
    }
  }

  @Override
  public TVector plusBy(long index, double x) {
    int partIndex = (int)((index + makeup) / splitLen) + makeupPos;
    if (vectors[partIndex] == null) {
      vectors[partIndex] = initComponentVector(initCapacity);
    }
    vectors[partIndex].plusBy(index, x);
    return this;
  }

  @Override
  public void set(long index, double x) {
    int partIndex = (int)((index + makeup) / splitLen) + makeupPos;
    if (vectors[partIndex] == null) {
      vectors[partIndex] = initComponentVector(initCapacity);
    }
    vectors[partIndex].set(index, x);
    //return this;
  }

  @Override
  public double get(long index) {
    int partIndex = (int)((index + makeup) / splitLen) + makeupPos;
    if (vectors[partIndex] == null) {
      return 0.0;
    } else {
      return vectors[partIndex].get(index);
    }
  }

  @Override public long[] getIndexes() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public double[] getValues() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector plusBy(TAbstractVector other) {
    if (other instanceof CompLongKeyDoubleVector) {
      return plusBy((CompLongKeyDoubleVector) other);
    } else if(other instanceof SparseLongKeyDoubleVector) {
      return plusBy((SparseLongKeyDoubleVector) other);
    } else if (other instanceof  SparseLongKeyDummyVector) {
      return plusBy((SparseLongKeyDummyVector) other);
    } else if (other instanceof SparseLongKeySortedDoubleVector) {
      return plusBy((SparseLongKeySortedDoubleVector) other);
    } else if (other instanceof  SparseDummyVector) {
      return plusBy((SparseDummyVector) other);
    } else if (other instanceof SparseDoubleSortedVector) {
      return plusBy((SparseDoubleSortedVector) other);
    }
    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private TVector plusBy(CompLongKeyDoubleVector other) {
    PlusByOp op = new PlusByOp(vectors, other.vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    op.join();
    return this;
  }

  private TVector plusBy(SparseLongKeyDoubleVector other) {
    ObjectIterator<Long2DoubleMap.Entry>
      iter = other.getIndexToValueMap().long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      plusBy(entry.getLongKey(), entry.getDoubleValue());
    }
    return this;
  }

  private TVector plusBy(SparseDoubleSortedVector other) {
    int [] indexes = other.getIndices();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      plusBy(indexes[i], values[i]);
    }
    return this;
  }

  private TVector plusBy(SparseLongKeySortedDoubleVector other) {
    long [] indexes = other.getIndexes();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      plusBy(indexes[i], values[i]);
    }
    return this;
  }

  private TVector plusBy(SparseLongKeyDummyVector other) {
    long [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      plusBy(indexes[i], 1);
    }
    return this;
  }

  private TVector plusBy(SparseDummyVector other) {
    int [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      plusBy(indexes[i], 1);
    }
    return this;
  }


  @Override public TVector plusBy(TAbstractVector other, double x) {
    if (other instanceof CompLongKeyDoubleVector) {
      return plusBy((CompLongKeyDoubleVector) other, x);
    } else if(other instanceof SparseLongKeyDoubleVector) {
      return plusBy((SparseLongKeyDoubleVector) other, x);
    } else if (other instanceof  SparseLongKeyDummyVector) {
      return plusBy((SparseLongKeyDummyVector) other, x);
    } else if (other instanceof SparseLongKeySortedDoubleVector) {
      return plusBy((SparseLongKeySortedDoubleVector) other, x);
    } else if (other instanceof  SparseDummyVector) {
      return plusBy((SparseDummyVector) other, x);
    } else if (other instanceof SparseDoubleSortedVector) {
      return plusBy((SparseDoubleSortedVector) other, x);
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private TVector plusBy(CompLongKeyDoubleVector other, double x) {
    PlusByWithFactorOp op = new PlusByWithFactorOp(vectors, other.vectors, 0, splitNum, x);
    MatrixOpExecutors.execute(op);
    op.join();
    return this;
  }

  private TVector plusBy(SparseLongKeyDoubleVector other, double x) {
    ObjectIterator<Long2DoubleMap.Entry>
      iter = other.getIndexToValueMap().long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      plusBy(entry.getLongKey(), entry.getDoubleValue() * x);
    }
    return this;
  }

  private TVector plusBy(SparseLongKeySortedDoubleVector other, double x) {
    long [] indexes = other.getIndexes();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      plusBy(indexes[i], values[i] * x);
    }
    return this;
  }

  private TVector plusBy(SparseDoubleSortedVector other, double x) {
    int [] indexes = other.getIndices();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      plusBy(indexes[i], values[i] * x);
    }
    return this;
  }

  private TVector plusBy(SparseLongKeyDummyVector other, double x) {
    long [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      plusBy(indexes[i], x);
    }
    return this;
  }

  private TVector plusBy(SparseDummyVector other, double x) {
    int [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      plusBy(indexes[i], x);
    }
    return this;
  }

  @Override public TVector plus(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector plus(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public double dot(TAbstractVector other) {
    if (other instanceof CompLongKeyDoubleVector) {
      return dot((CompLongKeyDoubleVector) other);
    } else if(other instanceof SparseLongKeyDoubleVector) {
      return dot((SparseLongKeyDoubleVector) other);
    } else if (other instanceof  SparseLongKeyDummyVector) {
      return dot((SparseLongKeyDummyVector) other);
    } else if (other instanceof SparseLongKeySortedDoubleVector) {
      return dot((SparseLongKeySortedDoubleVector) other);
    } else if (other instanceof  SparseDummyVector) {
      return dot((SparseDummyVector) other);
    } else if (other instanceof SparseDoubleSortedVector) {
      return dot((SparseDoubleSortedVector) other);
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(CompLongKeyDoubleVector other) {
    DotOp op = new DotOp(vectors, other.vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    return op.join();
  }

  private double dot(SparseLongKeyDoubleVector other) {
    double dotValue = 0.0;
    ObjectIterator<Long2DoubleMap.Entry>
      iter = other.getIndexToValueMap().long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      dotValue += get(entry.getLongKey()) * entry.getDoubleValue();
    }
    return dotValue;
  }

  private double dot(SparseLongKeySortedDoubleVector other) {
    double dotValue = 0.0;
    long [] indexes = other.getIndexes();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      dotValue += get(indexes[i]) * values[i];
    }
    return dotValue;
  }

  private double dot(SparseLongKeyDummyVector other) {
    double dotValue = 0.0;
    long [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      dotValue += get(indexes[i]);
    }
    return dotValue;
  }

  private double dot(SparseDoubleSortedVector other) {
    double dotValue = 0.0;
    int [] indexes = other.getIndices();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      dotValue += get((long)indexes[i]) * values[i];
    }
    return dotValue;
  }

  private double dot(SparseDummyVector other) {
    double dotValue = 0.0;
    int [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      dotValue += get(indexes[i]);
    }
    return dotValue;
  }

  @Override public TVector times(double x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector timesBy(double x) {
    TimesByOp op = new TimesByOp(vectors, 0, splitNum, x);
    MatrixOpExecutors.execute(op);
    op.join();
    return this;
  }

  @Override public TVector filter(double x) {
    throw new UnsupportedOperationException("Unsupport operation");
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

  @Override public double norm() {
    NormOp op = new NormOp(vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    return op.join();
  }

  @Override public double sparsity() {
    if (getLongDim() == -1) {
      return (double)nonZeroNumber() / Long.MAX_VALUE / 2;
    } else {
      return (double)nonZeroNumber() / getLongDim();
    }
  }

  @Override public int size() {
    int ret = 0;
    for (int i = 0; i < splitNum; i++) {
      if (vectors[i] != null) {
        ret += vectors[i].size();
      }
    }
    return ret;
  }

  public double sum() {
    SumOp op = new SumOp(vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    return op.join();
  }

  /**
   * Get the splits
   * @return the splits
   */
  public TLongDoubleVector[] getSplits() {
    return vectors;
  }

  /**
   * Get the partitions
   * @return the partitions
   */
  public PartitionKey[] getPartKeys() {
    return partKeys;
  }

  @Override
  public TLongDoubleVector elemUpdate(LongDoubleElemUpdater updater, ElemUpdateParam param) {
    ElementUpdateOp op = new ElementUpdateOp(vectors, 0, splitNum, updater, param);
    MatrixOpExecutors.execute(op);
    return this;
  }
}
