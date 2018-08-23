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


package com.tencent.angel.ml.math2;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;

public class VFactory {

  public static IntDoubleVector denseDoubleVector(int matrixId, int rowId, int clock, int dim) {
    IntDoubleVectorStorage storage = new IntDoubleDenseVectorStorage(dim);
    return new IntDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntDoubleVector denseDoubleVector(int dim) {
    IntDoubleVectorStorage storage = new IntDoubleDenseVectorStorage(dim);
    return new IntDoubleVector(0, 0, 0, dim, storage);
  }

  public static IntDoubleVector denseDoubleVector(int matrixId, int rowId, int clock,
    double[] values) {
    IntDoubleVectorStorage storage = new IntDoubleDenseVectorStorage(values);
    return new IntDoubleVector(matrixId, rowId, clock, values.length, storage);
  }

  public static IntDoubleVector denseDoubleVector(double[] values) {
    IntDoubleVectorStorage storage = new IntDoubleDenseVectorStorage(values);
    return new IntDoubleVector(0, 0, 0, values.length, storage);
  }

  public static IntFloatVector denseFloatVector(int matrixId, int rowId, int clock, int dim) {
    IntFloatVectorStorage storage = new IntFloatDenseVectorStorage(dim);
    return new IntFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntFloatVector denseFloatVector(int dim) {
    IntFloatVectorStorage storage = new IntFloatDenseVectorStorage(dim);
    return new IntFloatVector(0, 0, 0, dim, storage);
  }

  public static IntFloatVector denseFloatVector(int matrixId, int rowId, int clock,
    float[] values) {
    IntFloatVectorStorage storage = new IntFloatDenseVectorStorage(values);
    return new IntFloatVector(matrixId, rowId, clock, values.length, storage);
  }

  public static IntFloatVector denseFloatVector(float[] values) {
    IntFloatVectorStorage storage = new IntFloatDenseVectorStorage(values);
    return new IntFloatVector(0, 0, 0, values.length, storage);
  }

  public static IntLongVector denseLongVector(int matrixId, int rowId, int clock, int dim) {
    IntLongVectorStorage storage = new IntLongDenseVectorStorage(dim);
    return new IntLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntLongVector denseLongVector(int dim) {
    IntLongVectorStorage storage = new IntLongDenseVectorStorage(dim);
    return new IntLongVector(0, 0, 0, dim, storage);
  }

  public static IntLongVector denseLongVector(int matrixId, int rowId, int clock, long[] values) {
    IntLongVectorStorage storage = new IntLongDenseVectorStorage(values);
    return new IntLongVector(matrixId, rowId, clock, values.length, storage);
  }

  public static IntLongVector denseLongVector(long[] values) {
    IntLongVectorStorage storage = new IntLongDenseVectorStorage(values);
    return new IntLongVector(0, 0, 0, values.length, storage);
  }

  public static IntIntVector denseIntVector(int matrixId, int rowId, int clock, int dim) {
    IntIntVectorStorage storage = new IntIntDenseVectorStorage(dim);
    return new IntIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntIntVector denseIntVector(int dim) {
    IntIntVectorStorage storage = new IntIntDenseVectorStorage(dim);
    return new IntIntVector(0, 0, 0, dim, storage);
  }

  public static IntIntVector denseIntVector(int matrixId, int rowId, int clock, int[] values) {
    IntIntVectorStorage storage = new IntIntDenseVectorStorage(values);
    return new IntIntVector(matrixId, rowId, clock, values.length, storage);
  }

  public static IntIntVector denseIntVector(int[] values) {
    IntIntVectorStorage storage = new IntIntDenseVectorStorage(values);
    return new IntIntVector(0, 0, 0, values.length, storage);
  }

  public static IntDoubleVector sparseDoubleVector(int matrixId, int rowId, int clock, int dim) {
    IntDoubleVectorStorage storage = new IntDoubleSparseVectorStorage(dim);
    return new IntDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntDoubleVector sparseDoubleVector(int dim) {
    IntDoubleVectorStorage storage = new IntDoubleSparseVectorStorage(dim);
    return new IntDoubleVector(0, 0, 0, dim, storage);
  }

  public static IntDoubleVector sparseDoubleVector(int matrixId, int rowId, int clock, int dim,
    int capacity) {
    IntDoubleVectorStorage storage = new IntDoubleSparseVectorStorage(dim, capacity);
    return new IntDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntDoubleVector sparseDoubleVector(int dim, int capacity) {
    IntDoubleVectorStorage storage = new IntDoubleSparseVectorStorage(dim, capacity);
    return new IntDoubleVector(0, 0, 0, dim, storage);
  }

  public static IntDoubleVector sparseDoubleVector(int matrixId, int rowId, int clock, int dim,
    int[] indices, double[] values) {
    IntDoubleVectorStorage storage = new IntDoubleSparseVectorStorage(dim, indices, values);
    return new IntDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntDoubleVector sparseDoubleVector(int dim, int[] indices, double[] values) {
    IntDoubleVectorStorage storage = new IntDoubleSparseVectorStorage(dim, indices, values);
    return new IntDoubleVector(0, 0, 0, dim, storage);
  }

  public static LongDoubleVector sparseLongKeyDoubleVector(int matrixId, int rowId, int clock,
    long dim) {
    LongDoubleVectorStorage storage = new LongDoubleSparseVectorStorage(dim);
    return new LongDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongDoubleVector sparseLongKeyDoubleVector(long dim) {
    LongDoubleVectorStorage storage = new LongDoubleSparseVectorStorage(dim);
    return new LongDoubleVector(0, 0, 0, dim, storage);
  }

  public static LongDoubleVector sparseLongKeyDoubleVector(int matrixId, int rowId, int clock,
    long dim, int capacity) {
    LongDoubleVectorStorage storage = new LongDoubleSparseVectorStorage(dim, capacity);
    return new LongDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongDoubleVector sparseLongKeyDoubleVector(long dim, int capacity) {
    LongDoubleVectorStorage storage = new LongDoubleSparseVectorStorage(dim, capacity);
    return new LongDoubleVector(0, 0, 0, dim, storage);
  }

  public static LongDoubleVector sparseLongKeyDoubleVector(int matrixId, int rowId, int clock,
    long dim, long[] indices, double[] values) {
    LongDoubleVectorStorage storage = new LongDoubleSparseVectorStorage(dim, indices, values);
    return new LongDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongDoubleVector sparseLongKeyDoubleVector(long dim, long[] indices,
    double[] values) {
    LongDoubleVectorStorage storage = new LongDoubleSparseVectorStorage(dim, indices, values);
    return new LongDoubleVector(0, 0, 0, dim, storage);
  }

  public static IntFloatVector sparseFloatVector(int matrixId, int rowId, int clock, int dim) {
    IntFloatVectorStorage storage = new IntFloatSparseVectorStorage(dim);
    return new IntFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntFloatVector sparseFloatVector(int dim) {
    IntFloatVectorStorage storage = new IntFloatSparseVectorStorage(dim);
    return new IntFloatVector(0, 0, 0, dim, storage);
  }

  public static IntFloatVector sparseFloatVector(int matrixId, int rowId, int clock, int dim,
    int capacity) {
    IntFloatVectorStorage storage = new IntFloatSparseVectorStorage(dim, capacity);
    return new IntFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntFloatVector sparseFloatVector(int dim, int capacity) {
    IntFloatVectorStorage storage = new IntFloatSparseVectorStorage(dim, capacity);
    return new IntFloatVector(0, 0, 0, dim, storage);
  }

  public static IntFloatVector sparseFloatVector(int matrixId, int rowId, int clock, int dim,
    int[] indices, float[] values) {
    IntFloatVectorStorage storage = new IntFloatSparseVectorStorage(dim, indices, values);
    return new IntFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntFloatVector sparseFloatVector(int dim, int[] indices, float[] values) {
    IntFloatVectorStorage storage = new IntFloatSparseVectorStorage(dim, indices, values);
    return new IntFloatVector(0, 0, 0, dim, storage);
  }

  public static LongFloatVector sparseLongKeyFloatVector(int matrixId, int rowId, int clock,
    long dim) {
    LongFloatVectorStorage storage = new LongFloatSparseVectorStorage(dim);
    return new LongFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongFloatVector sparseLongKeyFloatVector(long dim) {
    LongFloatVectorStorage storage = new LongFloatSparseVectorStorage(dim);
    return new LongFloatVector(0, 0, 0, dim, storage);
  }

  public static LongFloatVector sparseLongKeyFloatVector(int matrixId, int rowId, int clock,
    long dim, int capacity) {
    LongFloatVectorStorage storage = new LongFloatSparseVectorStorage(dim, capacity);
    return new LongFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongFloatVector sparseLongKeyFloatVector(long dim, int capacity) {
    LongFloatVectorStorage storage = new LongFloatSparseVectorStorage(dim, capacity);
    return new LongFloatVector(0, 0, 0, dim, storage);
  }

  public static LongFloatVector sparseLongKeyFloatVector(int matrixId, int rowId, int clock,
    long dim, long[] indices, float[] values) {
    LongFloatVectorStorage storage = new LongFloatSparseVectorStorage(dim, indices, values);
    return new LongFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongFloatVector sparseLongKeyFloatVector(long dim, long[] indices, float[] values) {
    LongFloatVectorStorage storage = new LongFloatSparseVectorStorage(dim, indices, values);
    return new LongFloatVector(0, 0, 0, dim, storage);
  }

  public static IntLongVector sparseLongVector(int matrixId, int rowId, int clock, int dim) {
    IntLongVectorStorage storage = new IntLongSparseVectorStorage(dim);
    return new IntLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntLongVector sparseLongVector(int dim) {
    IntLongVectorStorage storage = new IntLongSparseVectorStorage(dim);
    return new IntLongVector(0, 0, 0, dim, storage);
  }

  public static IntLongVector sparseLongVector(int matrixId, int rowId, int clock, int dim,
    int capacity) {
    IntLongVectorStorage storage = new IntLongSparseVectorStorage(dim, capacity);
    return new IntLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntLongVector sparseLongVector(int dim, int capacity) {
    IntLongVectorStorage storage = new IntLongSparseVectorStorage(dim, capacity);
    return new IntLongVector(0, 0, 0, dim, storage);
  }

  public static IntLongVector sparseLongVector(int matrixId, int rowId, int clock, int dim,
    int[] indices, long[] values) {
    IntLongVectorStorage storage = new IntLongSparseVectorStorage(dim, indices, values);
    return new IntLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntLongVector sparseLongVector(int dim, int[] indices, long[] values) {
    IntLongVectorStorage storage = new IntLongSparseVectorStorage(dim, indices, values);
    return new IntLongVector(0, 0, 0, dim, storage);
  }

  public static LongLongVector sparseLongKeyLongVector(int matrixId, int rowId, int clock,
    long dim) {
    LongLongVectorStorage storage = new LongLongSparseVectorStorage(dim);
    return new LongLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongLongVector sparseLongKeyLongVector(long dim) {
    LongLongVectorStorage storage = new LongLongSparseVectorStorage(dim);
    return new LongLongVector(0, 0, 0, dim, storage);
  }

  public static LongLongVector sparseLongKeyLongVector(int matrixId, int rowId, int clock, long dim,
    int capacity) {
    LongLongVectorStorage storage = new LongLongSparseVectorStorage(dim, capacity);
    return new LongLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongLongVector sparseLongKeyLongVector(long dim, int capacity) {
    LongLongVectorStorage storage = new LongLongSparseVectorStorage(dim, capacity);
    return new LongLongVector(0, 0, 0, dim, storage);
  }

  public static LongLongVector sparseLongKeyLongVector(int matrixId, int rowId, int clock, long dim,
    long[] indices, long[] values) {
    LongLongVectorStorage storage = new LongLongSparseVectorStorage(dim, indices, values);
    return new LongLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongLongVector sparseLongKeyLongVector(long dim, long[] indices, long[] values) {
    LongLongVectorStorage storage = new LongLongSparseVectorStorage(dim, indices, values);
    return new LongLongVector(0, 0, 0, dim, storage);
  }

  public static IntIntVector sparseIntVector(int matrixId, int rowId, int clock, int dim) {
    IntIntVectorStorage storage = new IntIntSparseVectorStorage(dim);
    return new IntIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntIntVector sparseIntVector(int dim) {
    IntIntVectorStorage storage = new IntIntSparseVectorStorage(dim);
    return new IntIntVector(0, 0, 0, dim, storage);
  }

  public static IntIntVector sparseIntVector(int matrixId, int rowId, int clock, int dim,
    int capacity) {
    IntIntVectorStorage storage = new IntIntSparseVectorStorage(dim, capacity);
    return new IntIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntIntVector sparseIntVector(int dim, int capacity) {
    IntIntVectorStorage storage = new IntIntSparseVectorStorage(dim, capacity);
    return new IntIntVector(0, 0, 0, dim, storage);
  }

  public static IntIntVector sparseIntVector(int matrixId, int rowId, int clock, int dim,
    int[] indices, int[] values) {
    IntIntVectorStorage storage = new IntIntSparseVectorStorage(dim, indices, values);
    return new IntIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntIntVector sparseIntVector(int dim, int[] indices, int[] values) {
    IntIntVectorStorage storage = new IntIntSparseVectorStorage(dim, indices, values);
    return new IntIntVector(0, 0, 0, dim, storage);
  }

  public static LongIntVector sparseLongKeyIntVector(int matrixId, int rowId, int clock, long dim) {
    LongIntVectorStorage storage = new LongIntSparseVectorStorage(dim);
    return new LongIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongIntVector sparseLongKeyIntVector(long dim) {
    LongIntVectorStorage storage = new LongIntSparseVectorStorage(dim);
    return new LongIntVector(0, 0, 0, dim, storage);
  }

  public static LongIntVector sparseLongKeyIntVector(int matrixId, int rowId, int clock, long dim,
    int capacity) {
    LongIntVectorStorage storage = new LongIntSparseVectorStorage(dim, capacity);
    return new LongIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongIntVector sparseLongKeyIntVector(long dim, int capacity) {
    LongIntVectorStorage storage = new LongIntSparseVectorStorage(dim, capacity);
    return new LongIntVector(0, 0, 0, dim, storage);
  }

  public static LongIntVector sparseLongKeyIntVector(int matrixId, int rowId, int clock, long dim,
    long[] indices, int[] values) {
    LongIntVectorStorage storage = new LongIntSparseVectorStorage(dim, indices, values);
    return new LongIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongIntVector sparseLongKeyIntVector(long dim, long[] indices, int[] values) {
    LongIntVectorStorage storage = new LongIntSparseVectorStorage(dim, indices, values);
    return new LongIntVector(0, 0, 0, dim, storage);
  }

  public static IntDoubleVector sortedDoubleVector(int matrixId, int rowId, int clock, int dim) {
    IntDoubleSortedVectorStorage storage = new IntDoubleSortedVectorStorage(dim);
    return new IntDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntDoubleVector sortedDoubleVector(int dim) {
    IntDoubleSortedVectorStorage storage = new IntDoubleSortedVectorStorage(dim);
    return new IntDoubleVector(0, 0, 0, dim, storage);
  }

  public static IntDoubleVector sortedDoubleVector(int matrixId, int rowId, int clock, int dim,
    int capacity) {
    IntDoubleSortedVectorStorage storage = new IntDoubleSortedVectorStorage(dim, capacity);
    return new IntDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntDoubleVector sortedDoubleVector(int dim, int capacity) {
    IntDoubleSortedVectorStorage storage = new IntDoubleSortedVectorStorage(dim, capacity);
    return new IntDoubleVector(0, 0, 0, dim, storage);
  }

  public static IntDoubleVector sortedDoubleVector(int matrixId, int rowId, int clock, int dim,
    int size, int[] indices, double[] values) {
    IntDoubleSortedVectorStorage storage =
      new IntDoubleSortedVectorStorage(dim, size, indices, values);
    return new IntDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntDoubleVector sortedDoubleVector(int dim, int size, int[] indices,
    double[] values) {
    IntDoubleSortedVectorStorage storage =
      new IntDoubleSortedVectorStorage(dim, size, indices, values);
    return new IntDoubleVector(0, 0, 0, dim, storage);
  }

  public static IntDoubleVector sortedDoubleVector(int matrixId, int rowId, int clock, int dim,
    int[] indices, double[] values) {
    IntDoubleSortedVectorStorage storage = new IntDoubleSortedVectorStorage(dim, indices, values);
    return new IntDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntDoubleVector sortedDoubleVector(int dim, int[] indices, double[] values) {
    IntDoubleSortedVectorStorage storage = new IntDoubleSortedVectorStorage(dim, indices, values);
    return new IntDoubleVector(0, 0, 0, dim, storage);
  }

  public static LongDoubleVector sortedLongKeyDoubleVector(int matrixId, int rowId, int clock,
    long dim) {
    LongDoubleSortedVectorStorage storage = new LongDoubleSortedVectorStorage(dim);
    return new LongDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongDoubleVector sortedLongKeyDoubleVector(long dim) {
    LongDoubleSortedVectorStorage storage = new LongDoubleSortedVectorStorage(dim);
    return new LongDoubleVector(0, 0, 0, dim, storage);
  }

  public static LongDoubleVector sortedLongKeyDoubleVector(int matrixId, int rowId, int clock,
    long dim, int capacity) {
    LongDoubleSortedVectorStorage storage = new LongDoubleSortedVectorStorage(dim, capacity);
    return new LongDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongDoubleVector sortedLongKeyDoubleVector(long dim, int capacity) {
    LongDoubleSortedVectorStorage storage = new LongDoubleSortedVectorStorage(dim, capacity);
    return new LongDoubleVector(0, 0, 0, dim, storage);
  }

  public static LongDoubleVector sortedLongKeyDoubleVector(int matrixId, int rowId, int clock,
    long dim, int size, long[] indices, double[] values) {
    LongDoubleSortedVectorStorage storage =
      new LongDoubleSortedVectorStorage(dim, size, indices, values);
    return new LongDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongDoubleVector sortedLongKeyDoubleVector(long dim, int size, long[] indices,
    double[] values) {
    LongDoubleSortedVectorStorage storage =
      new LongDoubleSortedVectorStorage(dim, size, indices, values);
    return new LongDoubleVector(0, 0, 0, dim, storage);
  }

  public static LongDoubleVector sortedLongKeyDoubleVector(int matrixId, int rowId, int clock,
    long dim, long[] indices, double[] values) {
    LongDoubleSortedVectorStorage storage = new LongDoubleSortedVectorStorage(dim, indices, values);
    return new LongDoubleVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongDoubleVector sortedLongKeyDoubleVector(long dim, long[] indices,
    double[] values) {
    LongDoubleSortedVectorStorage storage = new LongDoubleSortedVectorStorage(dim, indices, values);
    return new LongDoubleVector(0, 0, 0, dim, storage);
  }

  public static IntFloatVector sortedFloatVector(int matrixId, int rowId, int clock, int dim) {
    IntFloatSortedVectorStorage storage = new IntFloatSortedVectorStorage(dim);
    return new IntFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntFloatVector sortedFloatVector(int dim) {
    IntFloatSortedVectorStorage storage = new IntFloatSortedVectorStorage(dim);
    return new IntFloatVector(0, 0, 0, dim, storage);
  }

  public static IntFloatVector sortedFloatVector(int matrixId, int rowId, int clock, int dim,
    int capacity) {
    IntFloatSortedVectorStorage storage = new IntFloatSortedVectorStorage(dim, capacity);
    return new IntFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntFloatVector sortedFloatVector(int dim, int capacity) {
    IntFloatSortedVectorStorage storage = new IntFloatSortedVectorStorage(dim, capacity);
    return new IntFloatVector(0, 0, 0, dim, storage);
  }

  public static IntFloatVector sortedFloatVector(int matrixId, int rowId, int clock, int dim,
    int size, int[] indices, float[] values) {
    IntFloatSortedVectorStorage storage =
      new IntFloatSortedVectorStorage(dim, size, indices, values);
    return new IntFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntFloatVector sortedFloatVector(int dim, int size, int[] indices, float[] values) {
    IntFloatSortedVectorStorage storage =
      new IntFloatSortedVectorStorage(dim, size, indices, values);
    return new IntFloatVector(0, 0, 0, dim, storage);
  }

  public static IntFloatVector sortedFloatVector(int matrixId, int rowId, int clock, int dim,
    int[] indices, float[] values) {
    IntFloatSortedVectorStorage storage = new IntFloatSortedVectorStorage(dim, indices, values);
    return new IntFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntFloatVector sortedFloatVector(int dim, int[] indices, float[] values) {
    IntFloatSortedVectorStorage storage = new IntFloatSortedVectorStorage(dim, indices, values);
    return new IntFloatVector(0, 0, 0, dim, storage);
  }

  public static LongFloatVector sortedLongKeyFloatVector(int matrixId, int rowId, int clock,
    long dim) {
    LongFloatSortedVectorStorage storage = new LongFloatSortedVectorStorage(dim);
    return new LongFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongFloatVector sortedLongKeyFloatVector(long dim) {
    LongFloatSortedVectorStorage storage = new LongFloatSortedVectorStorage(dim);
    return new LongFloatVector(0, 0, 0, dim, storage);
  }

  public static LongFloatVector sortedLongKeyFloatVector(int matrixId, int rowId, int clock,
    long dim, int capacity) {
    LongFloatSortedVectorStorage storage = new LongFloatSortedVectorStorage(dim, capacity);
    return new LongFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongFloatVector sortedLongKeyFloatVector(long dim, int capacity) {
    LongFloatSortedVectorStorage storage = new LongFloatSortedVectorStorage(dim, capacity);
    return new LongFloatVector(0, 0, 0, dim, storage);
  }

  public static LongFloatVector sortedLongKeyFloatVector(int matrixId, int rowId, int clock,
    long dim, int size, long[] indices, float[] values) {
    LongFloatSortedVectorStorage storage =
      new LongFloatSortedVectorStorage(dim, size, indices, values);
    return new LongFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongFloatVector sortedLongKeyFloatVector(long dim, int size, long[] indices,
    float[] values) {
    LongFloatSortedVectorStorage storage =
      new LongFloatSortedVectorStorage(dim, size, indices, values);
    return new LongFloatVector(0, 0, 0, dim, storage);
  }

  public static LongFloatVector sortedLongKeyFloatVector(int matrixId, int rowId, int clock,
    long dim, long[] indices, float[] values) {
    LongFloatSortedVectorStorage storage = new LongFloatSortedVectorStorage(dim, indices, values);
    return new LongFloatVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongFloatVector sortedLongKeyFloatVector(long dim, long[] indices, float[] values) {
    LongFloatSortedVectorStorage storage = new LongFloatSortedVectorStorage(dim, indices, values);
    return new LongFloatVector(0, 0, 0, dim, storage);
  }

  public static IntLongVector sortedLongVector(int matrixId, int rowId, int clock, int dim) {
    IntLongSortedVectorStorage storage = new IntLongSortedVectorStorage(dim);
    return new IntLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntLongVector sortedLongVector(int dim) {
    IntLongSortedVectorStorage storage = new IntLongSortedVectorStorage(dim);
    return new IntLongVector(0, 0, 0, dim, storage);
  }

  public static IntLongVector sortedLongVector(int matrixId, int rowId, int clock, int dim,
    int capacity) {
    IntLongSortedVectorStorage storage = new IntLongSortedVectorStorage(dim, capacity);
    return new IntLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntLongVector sortedLongVector(int dim, int capacity) {
    IntLongSortedVectorStorage storage = new IntLongSortedVectorStorage(dim, capacity);
    return new IntLongVector(0, 0, 0, dim, storage);
  }

  public static IntLongVector sortedLongVector(int matrixId, int rowId, int clock, int dim,
    int size, int[] indices, long[] values) {
    IntLongSortedVectorStorage storage = new IntLongSortedVectorStorage(dim, size, indices, values);
    return new IntLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntLongVector sortedLongVector(int dim, int size, int[] indices, long[] values) {
    IntLongSortedVectorStorage storage = new IntLongSortedVectorStorage(dim, size, indices, values);
    return new IntLongVector(0, 0, 0, dim, storage);
  }

  public static IntLongVector sortedLongVector(int matrixId, int rowId, int clock, int dim,
    int[] indices, long[] values) {
    IntLongSortedVectorStorage storage = new IntLongSortedVectorStorage(dim, indices, values);
    return new IntLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntLongVector sortedLongVector(int dim, int[] indices, long[] values) {
    IntLongSortedVectorStorage storage = new IntLongSortedVectorStorage(dim, indices, values);
    return new IntLongVector(0, 0, 0, dim, storage);
  }

  public static LongLongVector sortedLongKeyLongVector(int matrixId, int rowId, int clock,
    long dim) {
    LongLongSortedVectorStorage storage = new LongLongSortedVectorStorage(dim);
    return new LongLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongLongVector sortedLongKeyLongVector(long dim) {
    LongLongSortedVectorStorage storage = new LongLongSortedVectorStorage(dim);
    return new LongLongVector(0, 0, 0, dim, storage);
  }

  public static LongLongVector sortedLongKeyLongVector(int matrixId, int rowId, int clock, long dim,
    int capacity) {
    LongLongSortedVectorStorage storage = new LongLongSortedVectorStorage(dim, capacity);
    return new LongLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongLongVector sortedLongKeyLongVector(long dim, int capacity) {
    LongLongSortedVectorStorage storage = new LongLongSortedVectorStorage(dim, capacity);
    return new LongLongVector(0, 0, 0, dim, storage);
  }

  public static LongLongVector sortedLongKeyLongVector(int matrixId, int rowId, int clock, long dim,
    int size, long[] indices, long[] values) {
    LongLongSortedVectorStorage storage =
      new LongLongSortedVectorStorage(dim, size, indices, values);
    return new LongLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongLongVector sortedLongKeyLongVector(long dim, int size, long[] indices,
    long[] values) {
    LongLongSortedVectorStorage storage =
      new LongLongSortedVectorStorage(dim, size, indices, values);
    return new LongLongVector(0, 0, 0, dim, storage);
  }

  public static LongLongVector sortedLongKeyLongVector(int matrixId, int rowId, int clock, long dim,
    long[] indices, long[] values) {
    LongLongSortedVectorStorage storage = new LongLongSortedVectorStorage(dim, indices, values);
    return new LongLongVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongLongVector sortedLongKeyLongVector(long dim, long[] indices, long[] values) {
    LongLongSortedVectorStorage storage = new LongLongSortedVectorStorage(dim, indices, values);
    return new LongLongVector(0, 0, 0, dim, storage);
  }

  public static IntIntVector sortedIntVector(int matrixId, int rowId, int clock, int dim) {
    IntIntSortedVectorStorage storage = new IntIntSortedVectorStorage(dim);
    return new IntIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntIntVector sortedIntVector(int dim) {
    IntIntSortedVectorStorage storage = new IntIntSortedVectorStorage(dim);
    return new IntIntVector(0, 0, 0, dim, storage);
  }

  public static IntIntVector sortedIntVector(int matrixId, int rowId, int clock, int dim,
    int capacity) {
    IntIntSortedVectorStorage storage = new IntIntSortedVectorStorage(dim, capacity);
    return new IntIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntIntVector sortedIntVector(int dim, int capacity) {
    IntIntSortedVectorStorage storage = new IntIntSortedVectorStorage(dim, capacity);
    return new IntIntVector(0, 0, 0, dim, storage);
  }

  public static IntIntVector sortedIntVector(int matrixId, int rowId, int clock, int dim, int size,
    int[] indices, int[] values) {
    IntIntSortedVectorStorage storage = new IntIntSortedVectorStorage(dim, size, indices, values);
    return new IntIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntIntVector sortedIntVector(int dim, int size, int[] indices, int[] values) {
    IntIntSortedVectorStorage storage = new IntIntSortedVectorStorage(dim, size, indices, values);
    return new IntIntVector(0, 0, 0, dim, storage);
  }

  public static IntIntVector sortedIntVector(int matrixId, int rowId, int clock, int dim,
    int[] indices, int[] values) {
    IntIntSortedVectorStorage storage = new IntIntSortedVectorStorage(dim, indices, values);
    return new IntIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static IntIntVector sortedIntVector(int dim, int[] indices, int[] values) {
    IntIntSortedVectorStorage storage = new IntIntSortedVectorStorage(dim, indices, values);
    return new IntIntVector(0, 0, 0, dim, storage);
  }

  public static LongIntVector sortedLongKeyIntVector(int matrixId, int rowId, int clock, long dim) {
    LongIntSortedVectorStorage storage = new LongIntSortedVectorStorage(dim);
    return new LongIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongIntVector sortedLongKeyIntVector(long dim) {
    LongIntSortedVectorStorage storage = new LongIntSortedVectorStorage(dim);
    return new LongIntVector(0, 0, 0, dim, storage);
  }

  public static LongIntVector sortedLongKeyIntVector(int matrixId, int rowId, int clock, long dim,
    int capacity) {
    LongIntSortedVectorStorage storage = new LongIntSortedVectorStorage(dim, capacity);
    return new LongIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongIntVector sortedLongKeyIntVector(long dim, int capacity) {
    LongIntSortedVectorStorage storage = new LongIntSortedVectorStorage(dim, capacity);
    return new LongIntVector(0, 0, 0, dim, storage);
  }

  public static LongIntVector sortedLongKeyIntVector(int matrixId, int rowId, int clock, long dim,
    int size, long[] indices, int[] values) {
    LongIntSortedVectorStorage storage = new LongIntSortedVectorStorage(dim, size, indices, values);
    return new LongIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongIntVector sortedLongKeyIntVector(long dim, int size, long[] indices,
    int[] values) {
    LongIntSortedVectorStorage storage = new LongIntSortedVectorStorage(dim, size, indices, values);
    return new LongIntVector(0, 0, 0, dim, storage);
  }

  public static LongIntVector sortedLongKeyIntVector(int matrixId, int rowId, int clock, long dim,
    long[] indices, int[] values) {
    LongIntSortedVectorStorage storage = new LongIntSortedVectorStorage(dim, indices, values);
    return new LongIntVector(matrixId, rowId, clock, dim, storage);
  }

  public static LongIntVector sortedLongKeyIntVector(long dim, long[] indices, int[] values) {
    LongIntSortedVectorStorage storage = new LongIntSortedVectorStorage(dim, indices, values);
    return new LongIntVector(0, 0, 0, dim, storage);
  }

  public static IntDummyVector intDummyVector(int matrixId, int rowId, int clock, int dim,
    int[] values) {
    return new IntDummyVector(matrixId, rowId, clock, dim, values);
  }

  public static IntDummyVector intDummyVector(int dim, int[] values) {
    return new IntDummyVector(dim, values);
  }

  public static LongDummyVector longDummyVector(int matrixId, int rowId, int clock, long dim,
    long[] values) {
    return new LongDummyVector(matrixId, rowId, clock, dim, values);
  }

  public static LongDummyVector longDummyVector(long dim, long[] values) {
    return new LongDummyVector(dim, values);
  }

  //---------------------------------------------------------------
  public static CompIntDoubleVector compIntDoubleVector(int matrixId, int rowId, int clock, int dim,
    IntDoubleVector[] partitions, int subDim) {
    return new CompIntDoubleVector(matrixId, rowId, clock, dim, partitions, subDim);
  }

  public static CompIntDoubleVector compIntDoubleVector(int dim, IntDoubleVector[] partitions,
    int subDim) {
    return new CompIntDoubleVector(dim, partitions, subDim);
  }

  public static CompIntDoubleVector compIntDoubleVector(int matrixId, int rowId, int clock, int dim,
    IntDoubleVector[] partitions) {
    return new CompIntDoubleVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompIntDoubleVector compIntDoubleVector(int dim, IntDoubleVector[] partitions) {
    return new CompIntDoubleVector(dim, partitions);
  }

  public static CompIntDoubleVector compIntDoubleVector(int matrixId, int rowId, int clock, int dim,
    int subDim, StorageType storageType) {
    int numParts = (dim + subDim - 1) / subDim;
    IntDoubleVector[] partitions = new IntDoubleVector[numParts];
    switch (storageType) {
      case DENSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = denseDoubleVector(dim % subDim);
          } else {
            partitions[i] = denseDoubleVector(subDim);
          }
        }
        break;
      case SPARSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sparseDoubleVector(dim % subDim);
          } else {
            partitions[i] = sparseDoubleVector(subDim);
          }
        }
        break;
      case SORTED:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sortedDoubleVector(dim % subDim);
          } else {
            partitions[i] = sortedDoubleVector(subDim);
          }
        }
        break;
    }

    return new CompIntDoubleVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompIntDoubleVector compIntDoubleVector(int dim, int subDim,
    StorageType storageType) {
    return compIntDoubleVector(0, 0, 0, dim, subDim, storageType);
  }

  public static CompIntDoubleVector compIntDoubleVector(int matrixId, int rowId, int clock, int dim,
    int subDim) {
    return new CompIntDoubleVector(matrixId, rowId, clock, dim, subDim);
  }

  public static CompIntDoubleVector compIntDoubleVector(int dim, int subDim) {
    return new CompIntDoubleVector(dim, subDim);
  }

  public static CompIntFloatVector compIntFloatVector(int matrixId, int rowId, int clock, int dim,
    IntFloatVector[] partitions, int subDim) {
    return new CompIntFloatVector(matrixId, rowId, clock, dim, partitions, subDim);
  }

  public static CompIntFloatVector compIntFloatVector(int dim, IntFloatVector[] partitions,
    int subDim) {
    return new CompIntFloatVector(dim, partitions, subDim);
  }

  public static CompIntFloatVector compIntFloatVector(int matrixId, int rowId, int clock, int dim,
    IntFloatVector[] partitions) {
    return new CompIntFloatVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompIntFloatVector compIntFloatVector(int dim, IntFloatVector[] partitions) {
    return new CompIntFloatVector(dim, partitions);
  }

  public static CompIntFloatVector compIntFloatVector(int matrixId, int rowId, int clock, int dim,
    int subDim, StorageType storageType) {
    int numParts = (dim + subDim - 1) / subDim;
    IntFloatVector[] partitions = new IntFloatVector[numParts];
    switch (storageType) {
      case DENSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = denseFloatVector(dim % subDim);
          } else {
            partitions[i] = denseFloatVector(subDim);
          }
        }
        break;
      case SPARSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sparseFloatVector(dim % subDim);
          } else {
            partitions[i] = sparseFloatVector(subDim);
          }
        }
        break;
      case SORTED:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sortedFloatVector(dim % subDim);
          } else {
            partitions[i] = sortedFloatVector(subDim);
          }
        }
        break;
    }

    return new CompIntFloatVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompIntFloatVector compIntFloatVector(int dim, int subDim,
    StorageType storageType) {
    return compIntFloatVector(0, 0, 0, dim, subDim, storageType);
  }

  public static CompIntFloatVector compIntFloatVector(int matrixId, int rowId, int clock, int dim,
    int subDim) {
    return new CompIntFloatVector(matrixId, rowId, clock, dim, subDim);
  }

  public static CompIntFloatVector compIntFloatVector(int dim, int subDim) {
    return new CompIntFloatVector(dim, subDim);
  }

  public static CompIntLongVector compIntLongVector(int matrixId, int rowId, int clock, int dim,
    IntLongVector[] partitions, int subDim) {
    return new CompIntLongVector(matrixId, rowId, clock, dim, partitions, subDim);
  }

  public static CompIntLongVector compIntLongVector(int dim, IntLongVector[] partitions,
    int subDim) {
    return new CompIntLongVector(dim, partitions, subDim);
  }

  public static CompIntLongVector compIntLongVector(int matrixId, int rowId, int clock, int dim,
    IntLongVector[] partitions) {
    return new CompIntLongVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompIntLongVector compIntLongVector(int dim, IntLongVector[] partitions) {
    return new CompIntLongVector(dim, partitions);
  }

  public static CompIntLongVector compIntLongVector(int matrixId, int rowId, int clock, int dim,
    int subDim, StorageType storageType) {
    int numParts = (dim + subDim - 1) / subDim;
    IntLongVector[] partitions = new IntLongVector[numParts];
    switch (storageType) {
      case DENSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = denseLongVector(dim % subDim);
          } else {
            partitions[i] = denseLongVector(subDim);
          }
        }
        break;
      case SPARSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sparseLongVector(dim % subDim);
          } else {
            partitions[i] = sparseLongVector(subDim);
          }
        }
        break;
      case SORTED:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sortedLongVector(dim % subDim);
          } else {
            partitions[i] = sortedLongVector(subDim);
          }
        }
        break;
    }

    return new CompIntLongVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompIntLongVector compIntLongVector(int dim, int subDim, StorageType storageType) {
    return compIntLongVector(0, 0, 0, dim, subDim, storageType);
  }

  public static CompIntLongVector compIntLongVector(int matrixId, int rowId, int clock, int dim,
    int subDim) {
    return new CompIntLongVector(matrixId, rowId, clock, dim, subDim);
  }

  public static CompIntLongVector compIntLongVector(int dim, int subDim) {
    return new CompIntLongVector(dim, subDim);
  }

  public static CompIntIntVector compIntIntVector(int matrixId, int rowId, int clock, int dim,
    IntIntVector[] partitions, int subDim) {
    return new CompIntIntVector(matrixId, rowId, clock, dim, partitions, subDim);
  }

  public static CompIntIntVector compIntIntVector(int dim, IntIntVector[] partitions, int subDim) {
    return new CompIntIntVector(dim, partitions, subDim);
  }

  public static CompIntIntVector compIntIntVector(int matrixId, int rowId, int clock, int dim,
    IntIntVector[] partitions) {
    return new CompIntIntVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompIntIntVector compIntIntVector(int dim, IntIntVector[] partitions) {
    return new CompIntIntVector(dim, partitions);
  }

  public static CompIntIntVector compIntIntVector(int matrixId, int rowId, int clock, int dim,
    int subDim, StorageType storageType) {
    int numParts = (dim + subDim - 1) / subDim;
    IntIntVector[] partitions = new IntIntVector[numParts];
    switch (storageType) {
      case DENSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = denseIntVector(dim % subDim);
          } else {
            partitions[i] = denseIntVector(subDim);
          }
        }
        break;
      case SPARSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sparseIntVector(dim % subDim);
          } else {
            partitions[i] = sparseIntVector(subDim);
          }
        }
        break;
      case SORTED:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sortedIntVector(dim % subDim);
          } else {
            partitions[i] = sortedIntVector(subDim);
          }
        }
        break;
    }

    return new CompIntIntVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompIntIntVector compIntIntVector(int dim, int subDim, StorageType storageType) {
    return compIntIntVector(0, 0, 0, dim, subDim, storageType);
  }

  public static CompIntIntVector compIntIntVector(int matrixId, int rowId, int clock, int dim,
    int subDim) {
    return new CompIntIntVector(matrixId, rowId, clock, dim, subDim);
  }

  public static CompIntIntVector compIntIntVector(int dim, int subDim) {
    return new CompIntIntVector(dim, subDim);
  }

  public static CompLongDoubleVector compLongDoubleVector(int matrixId, int rowId, int clock,
    long dim, LongDoubleVector[] partitions, long subDim) {
    return new CompLongDoubleVector(matrixId, rowId, clock, dim, partitions, subDim);
  }

  public static CompLongDoubleVector compLongDoubleVector(long dim, LongDoubleVector[] partitions,
    long subDim) {
    return new CompLongDoubleVector(dim, partitions, subDim);
  }

  public static CompLongDoubleVector compLongDoubleVector(int matrixId, int rowId, int clock,
    long dim, LongDoubleVector[] partitions) {
    return new CompLongDoubleVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompLongDoubleVector compLongDoubleVector(long dim, LongDoubleVector[] partitions) {
    return new CompLongDoubleVector(dim, partitions);
  }

  public static CompLongDoubleVector compLongDoubleVector(int matrixId, int rowId, int clock,
    long dim, long subDim, StorageType storageType) {
    int numParts = (int) ((dim + subDim - 1) / subDim);
    LongDoubleVector[] partitions = new LongDoubleVector[numParts];
    switch (storageType) {
      case DENSE:
        throw new AngelException("DENSE is not supported!");
      case SPARSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sparseLongKeyDoubleVector(dim % subDim);
          } else {
            partitions[i] = sparseLongKeyDoubleVector(subDim);
          }
        }
        break;
      case SORTED:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sortedLongKeyDoubleVector(dim % subDim);
          } else {
            partitions[i] = sortedLongKeyDoubleVector(subDim);
          }
        }
        break;
    }

    return new CompLongDoubleVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompLongDoubleVector compLongDoubleVector(long dim, long subDim,
    StorageType storageType) {
    return compLongDoubleVector(0, 0, 0, dim, subDim, storageType);
  }

  public static CompLongDoubleVector compLongDoubleVector(int matrixId, int rowId, int clock,
    long dim, long subDim) {
    return new CompLongDoubleVector(matrixId, rowId, clock, dim, subDim);
  }

  public static CompLongDoubleVector compLongDoubleVector(long dim, long subDim) {
    return new CompLongDoubleVector(dim, subDim);
  }

  public static CompLongFloatVector compLongFloatVector(int matrixId, int rowId, int clock,
    long dim, LongFloatVector[] partitions, long subDim) {
    return new CompLongFloatVector(matrixId, rowId, clock, dim, partitions, subDim);
  }

  public static CompLongFloatVector compLongFloatVector(long dim, LongFloatVector[] partitions,
    long subDim) {
    return new CompLongFloatVector(dim, partitions, subDim);
  }

  public static CompLongFloatVector compLongFloatVector(int matrixId, int rowId, int clock,
    long dim, LongFloatVector[] partitions) {
    return new CompLongFloatVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompLongFloatVector compLongFloatVector(long dim, LongFloatVector[] partitions) {
    return new CompLongFloatVector(dim, partitions);
  }

  public static CompLongFloatVector compLongFloatVector(int matrixId, int rowId, int clock,
    long dim, long subDim, StorageType storageType) {
    int numParts = (int) ((dim + subDim - 1) / subDim);
    LongFloatVector[] partitions = new LongFloatVector[numParts];
    switch (storageType) {
      case DENSE:
        throw new AngelException("DENSE is not supported!");
      case SPARSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sparseLongKeyFloatVector(dim % subDim);
          } else {
            partitions[i] = sparseLongKeyFloatVector(subDim);
          }
        }
        break;
      case SORTED:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sortedLongKeyFloatVector(dim % subDim);
          } else {
            partitions[i] = sortedLongKeyFloatVector(subDim);
          }
        }
        break;
    }

    return new CompLongFloatVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompLongFloatVector compLongFloatVector(long dim, long subDim,
    StorageType storageType) {
    return compLongFloatVector(0, 0, 0, dim, subDim, storageType);
  }

  public static CompLongFloatVector compLongFloatVector(int matrixId, int rowId, int clock,
    long dim, long subDim) {
    return new CompLongFloatVector(matrixId, rowId, clock, dim, subDim);
  }

  public static CompLongFloatVector compLongFloatVector(long dim, long subDim) {
    return new CompLongFloatVector(dim, subDim);
  }

  public static CompLongLongVector compLongLongVector(int matrixId, int rowId, int clock, long dim,
    LongLongVector[] partitions, long subDim) {
    return new CompLongLongVector(matrixId, rowId, clock, dim, partitions, subDim);
  }

  public static CompLongLongVector compLongLongVector(long dim, LongLongVector[] partitions,
    long subDim) {
    return new CompLongLongVector(dim, partitions, subDim);
  }

  public static CompLongLongVector compLongLongVector(int matrixId, int rowId, int clock, long dim,
    LongLongVector[] partitions) {
    return new CompLongLongVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompLongLongVector compLongLongVector(long dim, LongLongVector[] partitions) {
    return new CompLongLongVector(dim, partitions);
  }

  public static CompLongLongVector compLongLongVector(int matrixId, int rowId, int clock, long dim,
    long subDim, StorageType storageType) {
    int numParts = (int) ((dim + subDim - 1) / subDim);
    LongLongVector[] partitions = new LongLongVector[numParts];
    switch (storageType) {
      case DENSE:
        throw new AngelException("DENSE is not supported!");
      case SPARSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sparseLongKeyLongVector(dim % subDim);
          } else {
            partitions[i] = sparseLongKeyLongVector(subDim);
          }
        }
        break;
      case SORTED:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sortedLongKeyLongVector(dim % subDim);
          } else {
            partitions[i] = sortedLongKeyLongVector(subDim);
          }
        }
        break;
    }

    return new CompLongLongVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompLongLongVector compLongLongVector(long dim, long subDim,
    StorageType storageType) {
    return compLongLongVector(0, 0, 0, dim, subDim, storageType);
  }

  public static CompLongLongVector compLongLongVector(int matrixId, int rowId, int clock, long dim,
    long subDim) {
    return new CompLongLongVector(matrixId, rowId, clock, dim, subDim);
  }

  public static CompLongLongVector compLongLongVector(long dim, long subDim) {
    return new CompLongLongVector(dim, subDim);
  }

  public static CompLongIntVector compLongIntVector(int matrixId, int rowId, int clock, long dim,
    LongIntVector[] partitions, long subDim) {
    return new CompLongIntVector(matrixId, rowId, clock, dim, partitions, subDim);
  }

  public static CompLongIntVector compLongIntVector(long dim, LongIntVector[] partitions,
    long subDim) {
    return new CompLongIntVector(dim, partitions, subDim);
  }

  public static CompLongIntVector compLongIntVector(int matrixId, int rowId, int clock, long dim,
    LongIntVector[] partitions) {
    return new CompLongIntVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompLongIntVector compLongIntVector(long dim, LongIntVector[] partitions) {
    return new CompLongIntVector(dim, partitions);
  }

  public static CompLongIntVector compLongIntVector(int matrixId, int rowId, int clock, long dim,
    long subDim, StorageType storageType) {
    int numParts = (int) ((dim + subDim - 1) / subDim);
    LongIntVector[] partitions = new LongIntVector[numParts];
    switch (storageType) {
      case DENSE:
        throw new AngelException("DENSE is not supported!");
      case SPARSE:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sparseLongKeyIntVector(dim % subDim);
          } else {
            partitions[i] = sparseLongKeyIntVector(subDim);
          }
        }
        break;
      case SORTED:
        for (int i = 0; i < numParts; i++) {
          if (i == numParts - 1 && dim % subDim > 0) {
            partitions[i] = sortedLongKeyIntVector(dim % subDim);
          } else {
            partitions[i] = sortedLongKeyIntVector(subDim);
          }
        }
        break;
    }

    return new CompLongIntVector(matrixId, rowId, clock, dim, partitions);
  }

  public static CompLongIntVector compLongIntVector(long dim, long subDim,
    StorageType storageType) {
    return compLongIntVector(0, 0, 0, dim, subDim, storageType);
  }

  public static CompLongIntVector compLongIntVector(int matrixId, int rowId, int clock, long dim,
    long subDim) {
    return new CompLongIntVector(matrixId, rowId, clock, dim, subDim);
  }

  public static CompLongIntVector compLongIntVector(long dim, long subDim) {
    return new CompLongIntVector(dim, subDim);
  }
}
