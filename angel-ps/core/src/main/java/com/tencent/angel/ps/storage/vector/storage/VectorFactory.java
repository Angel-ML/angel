package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;

public class VectorFactory {

  public static Vector getVector(RowType rowType, StorageMethod storageMethod, BasicType keyType,
      BasicType valueType, long dim, long size) {
    switch (rowType) {
      case T_INT_DENSE:
      case T_INT_SPARSE: {
        switch (storageMethod) {
          case DENSE:
            return new IntIntVector((int) dim, new IntIntDenseVectorStorage((int) size));
          case SPARSE:
            return new IntIntVector((int) dim,
                new IntIntSparseVectorStorage((int) dim, (int) size));
          case SORTED:
            return new IntIntVector((int) dim,
                new IntIntSortedVectorStorage((int) dim, (int) size));
          default:
            return new IntIntVector((int) dim, new IntIntDenseVectorStorage((int) size));
        }
      }

      case T_LONG_DENSE:
      case T_LONG_SPARSE: {
        switch (storageMethod) {
          case DENSE:
            return new IntLongVector((int) dim, new IntLongDenseVectorStorage((int) size));
          case SPARSE:
            return new IntLongVector((int) dim,
                new IntLongSparseVectorStorage((int) dim, (int) size));
          case SORTED:
            return new IntLongVector((int) dim,
                new IntLongSortedVectorStorage((int) dim, (int) size));
          default:
            return new IntLongVector((int) dim, new IntLongDenseVectorStorage((int) size));
        }
      }

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE: {
        switch (storageMethod) {
          case DENSE:
            return new IntFloatVector((int) dim, new IntFloatDenseVectorStorage((int) size));
          case SPARSE:
            return new IntFloatVector((int) dim,
                new IntFloatSparseVectorStorage((int) dim, (int) size));
          case SORTED:
            return new IntFloatVector((int) dim,
                new IntFloatSortedVectorStorage((int) dim, (int) size));
          default:
            return new IntFloatVector((int) dim, new IntFloatDenseVectorStorage((int) size));
        }
      }

      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE: {
        switch (storageMethod) {
          case DENSE:
            return new IntDoubleVector((int) dim, new IntDoubleDenseVectorStorage((int) size));
          case SPARSE:
            return new IntDoubleVector((int) dim,
                new IntDoubleSparseVectorStorage((int) dim, (int) size));
          case SORTED:
            return new IntDoubleVector((int) dim,
                new IntDoubleSortedVectorStorage((int) dim, (int) size));
          default:
            return new IntDoubleVector((int) dim, new IntDoubleDenseVectorStorage((int) size));
        }
      }

      case T_INT_SPARSE_LONGKEY: {
        switch (storageMethod) {
          case SPARSE:
            return new LongIntVector(dim, new LongIntSparseVectorStorage(dim, (int) size));
          case SORTED:
            return new LongIntVector(dim,
                new LongIntSortedVectorStorage(dim, (int) size));
          default:
            return new LongIntVector(dim, new LongIntSparseVectorStorage(dim, (int) size));
        }
      }

      case T_LONG_SPARSE_LONGKEY: {
        switch (storageMethod) {
          case SPARSE:
            return new LongLongVector(dim, new LongLongSparseVectorStorage(dim, (int) size));
          case SORTED:
            return new LongLongVector(dim,
                new LongLongSortedVectorStorage(dim, (int) size));
          default:
            return new LongLongVector(dim, new LongLongSparseVectorStorage(dim, (int) size));
        }
      }

      case T_FLOAT_SPARSE_LONGKEY: {
        switch (storageMethod) {
          case SPARSE:
            return new LongFloatVector(dim, new LongFloatSparseVectorStorage(dim, (int) size));
          case SORTED:
            return new LongFloatVector(dim,
                new LongFloatSortedVectorStorage(dim, (int) size));
          default:
            return new LongFloatVector(dim, new LongFloatSparseVectorStorage(dim, (int) size));
        }
      }

      case T_DOUBLE_SPARSE_LONGKEY: {
        switch (storageMethod) {
          case SPARSE:
            return new LongDoubleVector(dim, new LongDoubleSparseVectorStorage(dim, (int) size));
          case SORTED:
            return new LongDoubleVector(dim,
                new LongDoubleSortedVectorStorage(dim, (int) size));
          default:
            return new LongDoubleVector(dim, new LongDoubleSparseVectorStorage(dim, (int) size));
        }
      }

      default:
        throw new UnsupportedOperationException("Can not support row type " + rowType);
    }

  }
}

