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

import com.tencent.angel.ml.math2.storage.IntDoubleDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.math2.utils.RowType;

/**
 * Vector factor
 */
public class VectorFactory {

  /**
   * Get a vector
   *
   * @param rowType row type
   * @param storageMethod storage method
   * @param keyType index type
   * @param valueType value type
   * @param dim vector dimension
   * @param size vector size
   * @return vector
   */
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

