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

public class CompLongDoubleVector extends DoubleVector implements LongKeyVector, ComponentVector {
  private LongDoubleVector[] partitions;
  private int numPartitions;
  private long dim;
  private long subDim;

  public CompLongDoubleVector(int matrixId, int rowId, int clock, long dim,
    LongDoubleVector[] partitions, long subDim) {
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

  public CompLongDoubleVector(long dim, LongDoubleVector[] partitions, long subDim) {
    this(0, 0, 0, dim, partitions, subDim);
  }

  public CompLongDoubleVector(int matrixId, int rowId, int clock, long dim,
    LongDoubleVector[] partitions) {
    this(matrixId, rowId, clock, dim, partitions, -1);
  }

  public CompLongDoubleVector(long dim, LongDoubleVector[] partitions) {
    this(0, 0, 0, dim, partitions);
  }

  public CompLongDoubleVector(int matrixId, int rowId, int clock, long dim, long subDim) {
    assert subDim != -1;

    setMatrixId(matrixId);
    setRowId(rowId);
    setClock(clock);
    setStorage(null);
    this.numPartitions = (int) ((dim + subDim - 1) / subDim);
    this.partitions = new LongDoubleVector[numPartitions];
    this.dim = dim;
    this.subDim = subDim;
  }

  public CompLongDoubleVector(long dim, long subDim) {
    this(0, 0, 0, dim, subDim);
  }

  public void setPartitions(LongDoubleVector[] partitions) {
    assert partitions.length == this.partitions.length;
    this.partitions = partitions;
  }

  @Override public int getNumPartitions() {
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

  public double get(long idx) {
    int partIdx = (int) (idx / subDim);
    long subIdx = idx % subDim;
    return partitions[partIdx].get(subIdx);
  }

  @Override public boolean hasKey(long idx) {
    int partIdx = (int) (idx / subDim);
    return partitions[partIdx].hasKey(idx);
  }

  public void set(long idx, double value) {
    int partIdx = (int) (idx / subDim);
    long subIdx = idx % subDim;
    partitions[partIdx].set(subIdx, value);
  }

  public LongDoubleVector[] getPartitions() {
    return partitions;
  }

  public double max() {
    return (double) CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Max);
  }

  public double min() {
    return (double) CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Min);
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

  @Override public long numZeros() {
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
    for (LongDoubleVector part : partitions) {
      part.clear();
    }
  }

  @Override public CompLongDoubleVector clone() {
    LongDoubleVector[] newPartitions = new LongDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].clone();
    }
    return new CompLongDoubleVector(getMatrixId(), getRowId(), getClock(), (long) getDim(),
      newPartitions, subDim);
  }

  @Override public CompLongDoubleVector copy() {
    LongDoubleVector[] newPartitions = new LongDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].copy();
    }
    return new CompLongDoubleVector(getMatrixId(), getRowId(), getClock(), (long) getDim(),
      newPartitions, subDim);
  }

  @Override public int getTypeIndex() {
    return 4;
  }

  @Override public RowType getType() {
    return RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT;
  }

  @Override public Vector filter(double threshold) {
    LongDoubleVector[] newPartitions = new LongDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (LongDoubleVector) partitions[i].filter(threshold);
    }

    return new CompLongDoubleVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }

  @Override public Vector ifilter(double threshold) {
    LongDoubleVector[] newPartitions = new LongDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (LongDoubleVector) partitions[i].ifilter(threshold);
    }

    return new CompLongDoubleVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }

  @Override public Vector filterUp(double threshold) {
    LongDoubleVector[] newPartitions = new LongDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (LongDoubleVector) partitions[i].filterUp(threshold);
    }

    return new CompLongDoubleVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }
}
