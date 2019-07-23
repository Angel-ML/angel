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


package com.tencent.angel.ml.servingmath2.vector;

import com.tencent.angel.ml.servingmath2.exceptions.MathNotImplementedException;
import com.tencent.angel.ml.servingmath2.utils.RowType;

public class CompIntIntVector extends IntVector implements IntKeyVector, ComponentVector {

  private IntIntVector[] partitions;
  private int numPartitions;
  private int dim;
  private int subDim;

  public CompIntIntVector() {
    super();
  }

  public CompIntIntVector(int matrixId, int rowId, int clock, int dim, IntIntVector[] partitions,
      int subDim) {
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

  public CompIntIntVector(int dim, IntIntVector[] partitions, int subDim) {
    this(0, 0, 0, dim, partitions, subDim);
  }

  public CompIntIntVector(int matrixId, int rowId, int clock, int dim, IntIntVector[] partitions) {
    this(matrixId, rowId, clock, dim, partitions, -1);
  }

  public CompIntIntVector(int dim, IntIntVector[] partitions) {
    this(0, 0, 0, dim, partitions);
  }

  public CompIntIntVector(int matrixId, int rowId, int clock, int dim, int subDim) {
    assert subDim != -1;

    setMatrixId(matrixId);
    setRowId(rowId);
    setClock(clock);
    setStorage(null);
    this.numPartitions = (int) ((dim + subDim - 1) / subDim);
    this.partitions = new IntIntVector[numPartitions];
    this.dim = dim;
    this.subDim = subDim;
  }

  public CompIntIntVector(int dim, int subDim) {
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

  public int get(int idx) {
    int partIdx = (int) (idx / subDim);
    int subIdx = idx % subDim;
    return partitions[partIdx].get(subIdx);
  }

  @Override
  public boolean hasKey(int idx) {
    int partIdx = (int) (idx / subDim);
    return partitions[partIdx].hasKey(idx);
  }

  public void set(int idx, int value) {
    int partIdx = (int) (idx / subDim);
    int subIdx = idx % subDim;
    partitions[partIdx].set(subIdx, value);
  }

  public IntIntVector[] getPartitions() {
    return partitions;
  }

  public void setPartitions(IntIntVector[] partitions) {
    assert partitions.length == this.partitions.length;
    this.partitions = partitions;
  }

  public int max() {
    throw new MathNotImplementedException("Math Not Implemented Method For Component Vectors");
  }

  public int min() {
    throw new MathNotImplementedException("Math Not Implemented Method For Component Vectors");
  }

  public double sum() {
    throw new MathNotImplementedException("Math Not Implemented Method For Component Vectors");
  }

  public double average() {
    throw new MathNotImplementedException("Math Not Implemented Method For Component Vectors");
  }

  public double std() {
    throw new MathNotImplementedException("Math Not Implemented Method For Component Vectors");
  }

  public double norm() {
    throw new MathNotImplementedException("Math Not Implemented Method For Component Vectors");
  }

  @Override
  public int numZeros() {
    throw new MathNotImplementedException("Math Not Implemented Method For Component Vectors");
  }

  public int size() {
    throw new MathNotImplementedException("Math Not Implemented Method For Component Vectors");
  }

  public void clear() {
    matrixId = 0;
    rowId = 0;
    clock = 0;
    dim = 0;
    for (IntIntVector part : partitions) {
      part.clear();
    }
  }

  @Override
  public CompIntIntVector clone() {
    IntIntVector[] newPartitions = new IntIntVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].clone();
    }
    return new CompIntIntVector(getMatrixId(), getRowId(), getClock(), (int) getDim(),
        newPartitions, subDim);
  }

  @Override
  public CompIntIntVector copy() {
    IntIntVector[] newPartitions = new IntIntVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = partitions[i].copy();
    }
    return new CompIntIntVector(getMatrixId(), getRowId(), getClock(), (int) getDim(),
        newPartitions, subDim);
  }

  @Override
  public int getTypeIndex() {
    return 1;
  }

  @Override
  public RowType getType() {
    return RowType.T_INT_SPARSE_COMPONENT;
  }

  @Override
  public Vector filter(double threshold) {
    IntIntVector[] newPartitions = new IntIntVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (IntIntVector) partitions[i].filter(threshold);
    }

    return new CompIntIntVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }

  @Override
  public Vector ifilter(double threshold) {
    IntIntVector[] newPartitions = new IntIntVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (IntIntVector) partitions[i].ifilter(threshold);
    }

    return new CompIntIntVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }

  @Override
  public Vector filterUp(double threshold) {
    IntIntVector[] newPartitions = new IntIntVector[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      newPartitions[i] = (IntIntVector) partitions[i].filterUp(threshold);
    }

    return new CompIntIntVector(matrixId, rowId, clock, dim, newPartitions, subDim);
  }
}