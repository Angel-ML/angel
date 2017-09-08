package com.tencent.angel.ml.math.vector;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.executor.MatrixOpExecutors;
import com.tencent.angel.protobuf.generated.MLProtos;

/**
 * Component sparse double vector. It contains a group of {@link SparseDoubleVector},
 * which correspond to the partitions of the corresponding rows stored on the PS.
 */
public class CompSparseDoubleVector extends CompTDoubleVector{
  /**
   *
   * Create a CompSparseDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param nnz element number of the vector
   */
  public CompSparseDoubleVector(int matrixId, int rowIndex, int dim, int nnz) {
    super(matrixId, rowIndex, dim, nnz);
  }

  /**
   * Create a CompSparseDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param partKeys the partitions that contains this vector
   * @param vectors vector splits
   */
  public CompSparseDoubleVector(int matrixId, int rowIndex, int dim,  PartitionKey[] partKeys, TDoubleVector[] vectors) {
    super(matrixId, rowIndex, dim, partKeys, vectors);
  }

  /**
   *
   * Create a CompSparseDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public CompSparseDoubleVector(int matrixId, int rowIndex) {
    this(matrixId, rowIndex, -1, -1);
  }

  /**
   *
   * Create a CompSparseDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   */
  public CompSparseDoubleVector(int matrixId, int rowIndex, int dim) {
    this(matrixId, rowIndex, dim, -1);
  }

  @Override public TDoubleVector clone() {
    CompSparseDoubleVector clonedVector = new CompSparseDoubleVector(matrixId, rowId, dim, partKeys, new TDoubleVector[splitNum]);
    PlusByOp
      op = new PlusByOp(clonedVector.vectors, vectors, 0, splitNum);
    MatrixOpExecutors.execute(op);
    op.join();
    return clonedVector;
  }

  @Override public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE_COMPONENT;
  }

  @Override protected TDoubleVector initComponentVector() {
    return initComponentVector(initCapacity);
  }

  @Override protected TDoubleVector initComponentVector(int initCapacity) {
    SparseDoubleVector vector = new SparseDoubleVector(dim, initCapacity);
    vector.setMatrixId(matrixId);
    vector.setRowId(rowId);
    vector.setClock(clock);
    return vector;
  }

  @Override protected TDoubleVector initComponentVector(TDoubleVector vector) {
    if(vector instanceof SparseDoubleVector) {
      return vector.clone();
    }

    throw new UnsupportedOperationException("Unsupport operation: clone from " + vector.getClass().getSimpleName() + " to SparseDoubleVector ");
  }
}
