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

import com.tencent.angel.ml.math2.ufuncs.executor.comp.CompReduceExecutor;
import com.tencent.angel.ml.matrix.RowType;

public class CompLongFloatVector extends FloatVector implements LongKeyVector, ComponentVector {
  private LongFloatVector[] partitions;
  private int numPartitions;
  private long dim;
  private long subDim;

  public CompLongFloatVector() {
    super();
  }

  public CompLongFloatVector(int matrixId, int rowId, int clock, long dim, LongFloatVector[] partitions, long subDim) {
    setMatrixId(matrixId);
    setRowId(rowId);
    setClock(clock);
    setStorage(null);
    this.partitions = partitions;
    this.numPartitions = partitions.length;
    this.dim = dim;
    if (dim < 0) {
      this.subDim = partitions[0].getDim();
    } else if (subDim == -1) {
      this.subDim = (dim + partitions.length - 1) / partitions.length;
    } else {
      this.subDim = subDim;
    }

  }

  public CompLongFloatVector(long dim, LongFloatVector[] partitions, long subDim) {
    this(0, 0, 0, dim, partitions, subDim);
  }

  public CompLongFloatVector(int matrixId, int rowId, int clock, long dim, LongFloatVector[] partitions) {
    this(matrixId, rowId, clock, dim, partitions, -1);
  }

  public CompLongFloatVector(long dim, LongFloatVector[] partitions) {
    this(0, 0, 0, dim, partitions);
  }

  public CompLongFloatVector(int matrixId, int rowId, int clock, long dim, long subDim) {
    assert subDim != -1;

    setMatrixId(matrixId);
    setRowId(rowId);
    setClock(clock);
    setStorage(null);
    this.numPartitions = (int) ((dim + subDim - 1) / subDim);
    this.partitions = new LongFloatVector[numPartitions];
    this.dim = dim;
    this.subDim = subDim;
  }

  public CompLongFloatVector(long dim, long subDim) {
    this(0, 0, 0, dim, subDim);
  }

  @Override
  public int getNumPartitions() {
    return numPartitions;
  }

  public long getDim() {
    return dim;
  }

  public long dim() {
    return (long) getDim();
  }

  public long getSubDim() {
    return subDim;
  }

  public void setSubDim(long subDim) {
    assert subDim > 0;
    this.subDim = subDim;
  }

  public boolean isCompatable(ComponentVector other) {
    if (getTypeIndex() >= other.getTypeIndex() && numPartitions == other.getNumPartitions()) {
      if (other instanceof IntKeyVector) {
        return dim == ((IntKeyVector) other).getDim();
      } else {
        return dim == ((LongKeyVector) other).getDim();
      }
    } else {
      return false;
    }
  }

  public float get(long idx) {
    int partIdx = (int) (idx / subDim);
    long subIdx = idx % subDim;
    return partitions[partIdx].get(subIdx);
  }

  @Override
  public boolean hasKey(long idx) {
    int partIdx = (int) (idx / subDim);
    return partitions[partIdx].hasKey(idx);
  }

  public void set(long idx, float value) {
    int partIdx = (int) (idx / subDim);
    long subIdx = idx % subDim;
    partitions[partIdx].set(subIdx, value);
  }

  public LongFloatVector[] getPartitions() {
    return partitions;
  }

  public void setPartitions(LongFloatVector[] partitions) {
    assert partitions.length == this.partitions.length;
    this.partitions = partitions;
  }

  public float max() {
    return (float) CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Max);
  }

  public float min() {
    return (float) CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Min);
  }

  public double sum() {
    return CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Sum);
  }

  public double average() {
    return CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Avg);
  }

  public double std() {
    return CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Std);
  }

  public double norm() {
    return CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Norm);
  }

  @Override
  public long numZeros() {
    return (long) CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Numzeros);
  }

  public long size() {
    return (long) CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Size);
  }

  public void clear() {
    matrixId = 0;
    rowId = 0;
    clock = 0;
    dim = 0;
    for (LongFloatVector part : partitions) {
      part.clear();
    }
  }

  @Override
  public CompLongFloatVector clone() {
    LongFloatVector[] newPartitions = new LongFloatVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].clone();
    }
    return new CompLongFloatVector(getMatrixId(), getRowId(), getClock(), (long) getDim(), newPartitions, subDim);
  }

  @Override
  public CompLongFloatVector copy() {
    LongFloatVector[] newPartitions = new LongFloatVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].copy();
    }
    return new CompLongFloatVector(getMatrixId(), getRowId(), getClock(), (long) getDim(), newPartitions, subDim);
  }

  @Override
  public CompLongFloatVector emptyLike() {
    LongFloatVector[] newPartitions = new LongFloatVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].emptyLike();
    }
    return new CompLongFloatVector(getMatrixId(), getRowId(), getClock(), (int) getDim(),
        newPartitions, subDim);
  }

  @Override
  public int getTypeIndex() {
    return 3;
  }

  @Override
  public RowType getType() {
    return RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT;
  }

  @Override
  public Vector filter(double threshold) {
    LongFloatVector[] newPartitions = new LongFloatVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (LongFloatVector) partitions[i].filter(threshold);
    }

    return new CompLongFloatVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }

  @Override
  public Vector ifilter(double threshold) {
    LongFloatVector[] newPartitions = new LongFloatVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (LongFloatVector) partitions[i].ifilter(threshold);
    }

    return new CompLongFloatVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }

  @Override
  public Vector filterUp(double threshold) {
    LongFloatVector[] newPartitions = new LongFloatVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (LongFloatVector) partitions[i].filterUp(threshold);
    }

    return new CompLongFloatVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }
}
