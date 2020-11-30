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

public class CompIntDoubleVector extends DoubleVector implements IntKeyVector, ComponentVector {
  private IntDoubleVector[] partitions;
  private int numPartitions;
  private int dim;
  private int subDim;

  public CompIntDoubleVector() {
    super();
  }

  public CompIntDoubleVector(int matrixId, int rowId, int clock, int dim, IntDoubleVector[] partitions, int subDim) {
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

  public CompIntDoubleVector(int dim, IntDoubleVector[] partitions, int subDim) {
    this(0, 0, 0, dim, partitions, subDim);
  }

  public CompIntDoubleVector(int matrixId, int rowId, int clock, int dim, IntDoubleVector[] partitions) {
    this(matrixId, rowId, clock, dim, partitions, -1);
  }

  public CompIntDoubleVector(int dim, IntDoubleVector[] partitions) {
    this(0, 0, 0, dim, partitions);
  }

  public CompIntDoubleVector(int matrixId, int rowId, int clock, int dim, int subDim) {
    assert subDim != -1;

    setMatrixId(matrixId);
    setRowId(rowId);
    setClock(clock);
    setStorage(null);
    this.numPartitions = (int) ((dim + subDim - 1) / subDim);
    this.partitions = new IntDoubleVector[numPartitions];
    this.dim = dim;
    this.subDim = subDim;
  }

  public CompIntDoubleVector(int dim, int subDim) {
    this(0, 0, 0, dim, subDim);
  }

  @Override
  public int getNumPartitions() {
    return numPartitions;
  }

  public int getDim() {
    return dim;
  }

  public long dim() {
    return (long) getDim();
  }

  public int getSubDim() {
    return subDim;
  }

  public void setSubDim(int subDim) {
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

  public double get(int idx) {
    int partIdx = (int) (idx / subDim);
    int subIdx = idx % subDim;
    return partitions[partIdx].get(subIdx);
  }

  @Override
  public boolean hasKey(int idx) {
    int partIdx = (int) (idx / subDim);
    return partitions[partIdx].hasKey(idx);
  }

  public void set(int idx, double value) {
    int partIdx = (int) (idx / subDim);
    int subIdx = idx % subDim;
    partitions[partIdx].set(subIdx, value);
  }

  public IntDoubleVector[] getPartitions() {
    return partitions;
  }

  public void setPartitions(IntDoubleVector[] partitions) {
    assert partitions.length == this.partitions.length;
    this.partitions = partitions;
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

  @Override
  public int numZeros() {
    return (int) CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Numzeros);
  }

  public int size() {
    return (int) CompReduceExecutor.apply(this, CompReduceExecutor.ReduceOP.Size);
  }

  public void clear() {
    matrixId = 0;
    rowId = 0;
    clock = 0;
    dim = 0;
    for (IntDoubleVector part : partitions) {
      part.clear();
    }
  }

  @Override
  public CompIntDoubleVector clone() {
    IntDoubleVector[] newPartitions = new IntDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].clone();
    }
    return new CompIntDoubleVector(getMatrixId(), getRowId(), getClock(), (int) getDim(), newPartitions, subDim);
  }

  @Override
  public CompIntDoubleVector copy() {
    IntDoubleVector[] newPartitions = new IntDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].copy();
    }
    return new CompIntDoubleVector(getMatrixId(), getRowId(), getClock(), (int) getDim(), newPartitions, subDim);
  }

  @Override
  public CompIntDoubleVector emptyLike() {
    IntDoubleVector[] newPartitions = new IntDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].emptyLike();
    }
    return new CompIntDoubleVector(getMatrixId(), getRowId(), getClock(), (int) getDim(),
        newPartitions, subDim);
  }

  @Override
  public int getTypeIndex() {
    return 4;
  }

  @Override
  public RowType getType() {
    return RowType.T_DOUBLE_SPARSE_COMPONENT;
  }

  @Override
  public Vector filter(double threshold) {
    IntDoubleVector[] newPartitions = new IntDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (IntDoubleVector) partitions[i].filter(threshold);
    }

    return new CompIntDoubleVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }

  @Override
  public Vector ifilter(double threshold) {
    IntDoubleVector[] newPartitions = new IntDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (IntDoubleVector) partitions[i].ifilter(threshold);
    }

    return new CompIntDoubleVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }

  @Override
  public Vector filterUp(double threshold) {
    IntDoubleVector[] newPartitions = new IntDoubleVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (IntDoubleVector) partitions[i].filterUp(threshold);
    }

    return new CompIntDoubleVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }
}
