package com.tencent.angel.ml.math.vector;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.executor.MatrixOpExecutors;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

/**
 * Component sparse double vector with long key. It contains a group of {@link SparseDoubleLongKeyVector},
 * which correspond to the partitions of the corresponding rows stored on the PS.
 */
public class CompSparseDoubleLongKeyVector extends LongKeyDoubleVector {
  private static final Log LOG = LogFactory.getLog(SparseDoubleLongKeyVector.class);

  /** The splits of the row, they are sorted by the start column index*/
  private final SparseDoubleLongKeyVector[] vectors;

  /** The Partitions that contain this row, they are sorted by the start column index */
  private final PartitionKey[] partKeys;

  /** The number of splits */
  private final int splitNum;

  /** The column number in a split */
  private final long splitLen;

  /** The estimate capacity of a split */
  private final int initCapacity;

  class TimesByOp extends RecursiveAction {
    private final SparseDoubleLongKeyVector[] rowSplits;
    private final double factor;
    private final int startPos;
    private final int endPos;

    public TimesByOp(SparseDoubleLongKeyVector[] rowSplits, int startPos, int endPos,
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
    private final SparseDoubleLongKeyVector[] leftSplits;
    private final SparseDoubleLongKeyVector[] rightSplits;
    private final int startPos;
    private final int endPos;

    public PlusByOp(SparseDoubleLongKeyVector[] leftSplits, SparseDoubleLongKeyVector[] rightSplits,
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
          leftSplits[startPos] = new SparseDoubleLongKeyVector(rightSplits[startPos]);
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
    private final SparseDoubleLongKeyVector[] leftSplits;
    private final SparseDoubleLongKeyVector[] rightSplits;
    private final int startPos;
    private final int endPos;
    private final double factor;

    public PlusByWithFactorOp(SparseDoubleLongKeyVector[] leftSplits,
      SparseDoubleLongKeyVector[] rightSplits, int startPos, int endPos, double factor) {
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
          leftSplits[startPos] = new SparseDoubleLongKeyVector(rightSplits[startPos]);
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
    private final SparseDoubleLongKeyVector[] leftSplits;
    private final SparseDoubleLongKeyVector[] rightSplits;
    private final int startPos;
    private final int endPos;

    public DotOp(SparseDoubleLongKeyVector[] leftSplits, SparseDoubleLongKeyVector[] rightSplits,
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
    private final SparseDoubleLongKeyVector[] splits;
    private final int startPos;
    private final int endPos;

    public NNZCounterOp(SparseDoubleLongKeyVector[] splits, int startPos, int endPos) {
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
    private final SparseDoubleLongKeyVector[] splits;
    private final int startPos;
    private final int endPos;

    public SquaredNormOp(SparseDoubleLongKeyVector[] splits, int startPos, int endPos) {
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
    private final SparseDoubleLongKeyVector[] splits;
    private final int startPos;
    private final int endPos;

    public SumOp(SparseDoubleLongKeyVector[] splits, int startPos, int endPos) {
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
   * Create a CompSparseDoubleLongKeyVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param partKeys the partitions that contains this vector
   * @param splits vector splits
   */
  public CompSparseDoubleLongKeyVector(int matrixId, int rowIndex, long dim,
    PartitionKey[] partKeys, SparseDoubleLongKeyVector[] splits) {
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
  }

  /**
   * Create a CompSparseDoubleLongKeyVector
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public CompSparseDoubleLongKeyVector(int matrixId, int rowIndex) {
    this(matrixId, rowIndex, -1, -1);
  }

  /**
   * Create a CompSparseDoubleLongKeyVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   */
  public CompSparseDoubleLongKeyVector(int matrixId, int rowIndex, long dim) {
    this(matrixId, rowIndex, dim, -1);
  }

  /**
   *
   * Create a CompSparseDoubleLongKeyVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param nnz element number of the vector
   */
  public CompSparseDoubleLongKeyVector(int matrixId, int rowIndex, long dim, long nnz) {
    super(dim);
    setMatrixId(matrixId);
    setRowId(rowIndex);

    List<PartitionKey> partKeyList =
      PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId, rowIndex);
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
    vectors = new SparseDoubleLongKeyVector[splitNum];
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
  }

  @Override public TVector plusBy(long index, double x) {
    int partIndex = (int) (index / splitLen);
    if (vectors[partIndex] == null) {
      vectors[partIndex] = new SparseDoubleLongKeyVector(dim, initCapacity);
    }
    vectors[partIndex].plusBy(index, x);
    return this;
  }

  @Override public TVector set(long index, double x) {
    int partIndex = (int) (index / splitLen);
    if (vectors[partIndex] == null) {
      vectors[partIndex] = new SparseDoubleLongKeyVector(dim, initCapacity);
    }
    vectors[partIndex].set(index, x);
    return this;
  }

  @Override public double get(long index) {
    int partIndex = (int) (index / splitLen);
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
    if (other instanceof CompSparseDoubleLongKeyVector) {
      return plusBy((CompSparseDoubleLongKeyVector) other);
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private TVector plusBy(CompSparseDoubleLongKeyVector other) {
    LOG.info("plusBy, splitNum=" + splitNum);
    PlusByOp op = new PlusByOp(vectors, other.vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    op.join();
    LOG.info("after plusBy, sum=" + sum());
    return this;
  }

  @Override public TVector plusBy(TAbstractVector other, double x) {
    if (other instanceof CompSparseDoubleLongKeyVector) {
      return plusBy((CompSparseDoubleLongKeyVector) other, x);
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private TVector plusBy(CompSparseDoubleLongKeyVector other, double x) {
    LOG.info("plusBy, splitNum=" + splitNum);
    PlusByWithFactorOp op = new PlusByWithFactorOp(vectors, other.vectors, 0, splitNum, x);
    MatrixOpExecutors.execute(op);
    op.join();
    LOG.info("after plusBy, sum=" + sum());
    return this;
  }

  @Override public TVector plus(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector plus(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public double dot(TAbstractVector other) {
    if (other instanceof CompSparseDoubleLongKeyVector) {
      return dot((CompSparseDoubleLongKeyVector) other);
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: " + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(CompSparseDoubleLongKeyVector other) {
    DotOp op = new DotOp(vectors, other.vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    return op.join();
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

  @Override public TVector clone() {
    CompSparseDoubleLongKeyVector clonedVector =
      new CompSparseDoubleLongKeyVector(matrixId, rowId, dim, partKeys,
        new SparseDoubleLongKeyVector[splitNum]);
    PlusByOp op = new PlusByOp(clonedVector.vectors, vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    op.join();
    return clonedVector;
  }

  @Override public double sparsity() {
    if (getDimension() == -1) {
      return nonZeroNumber() / Long.MAX_VALUE / 2;
    } else {
      return nonZeroNumber() / getLongDim();
    }
  }

  @Override public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY;
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
  public SparseDoubleLongKeyVector[] getSplits() {
    return vectors;
  }

  /**
   * Get the partitions
   * @return the partitions
   */
  public PartitionKey[] getPartKeys() {
    return partKeys;
  }
}
