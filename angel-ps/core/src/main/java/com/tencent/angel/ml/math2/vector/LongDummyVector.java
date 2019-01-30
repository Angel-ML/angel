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


package com.tencent.angel.ml.math2.vector;

import static java.lang.StrictMath.sqrt;

import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.matrix.RowType;
import java.util.Arrays;
import org.apache.commons.lang.NotImplementedException;

public class LongDummyVector extends Vector implements LongKeyVector, SimpleVector {

  private long dim;
  private long[] indices;

  public LongDummyVector() {
    super();
  }

  public LongDummyVector(int matrixId, int rowId, int clock, long dim, long[] values) {
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.clock = clock;
    this.dim = dim;
    this.indices = values;
  }

  public LongDummyVector(long dim, long[] values) {
    this(0, 0, 0, dim, values);
  }

  public int get(long idx) {
    if (idx < 0 || idx > dim - 1) {
      throw new ArrayIndexOutOfBoundsException();
    } else if (idx < indices[0] && idx > indices[indices.length - 1]) {
      return 0;
    } else {
      int i = Arrays.binarySearch(indices, idx);
      return i >= 0 ? 1 : 0;
    }
  }

  public boolean hasKey(long idx) {
    if (idx < 0 || idx > dim - 1) {
      throw new ArrayIndexOutOfBoundsException();
    } else {
      return idx >= indices[0] && idx <= indices[indices.length - 1]
          && Arrays.binarySearch(indices, idx) >= 0;
    }
  }

  public long[] getIndices() {
    return indices;
  }

  public long getDim() {
    return dim;
  }

  public void setDim(long dim) {
    this.dim = dim;
  }

  public long dim() {
    return getDim();
  }

  @Override
  public long numZeros() {
    return dim - indices.length;
  }

  @Override
  public long size() {
    return indices.length;
  }

  @Override
  public double sum() {
    return size();
  }

  @Override
  public double std() {
    double avg = average();
    return sqrt(avg - avg * avg);
  }

  @Override
  public double average() {
    return 1.0 * size() / getDim();
  }

  @Override
  public double norm() {
    return sqrt(size());
  }

  @Override
  public void clear() {
    for (int i = 0; i < indices.length; i++) {
      indices[i] = 0;
    }
  }

  @Override
  public boolean isDense() {
    return false;
  }

  @Override
  public boolean isSparse() {
    return false;
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public Vector filter(double threshold) {
    throw new NotImplementedException();
  }

  @Override
  public Vector ifilter(double threshold) {
    throw new NotImplementedException();
  }

  @Override
  public Vector filterUp(double threshold) {
    throw new NotImplementedException();
  }

  @Override
  public LongDummyVector copy() {
    return new LongDummyVector(matrixId, rowId, clock, dim, ArrayCopy.copy(indices));
  }

  @Override
  public RowType getType() {
    return RowType.T_DOUBLE_SPARSE_LONGKEY;
  }
}