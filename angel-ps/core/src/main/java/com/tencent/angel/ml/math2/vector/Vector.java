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

import com.tencent.angel.ml.math2.MathObject;
import com.tencent.angel.ml.math2.storage.IntKeyVectorStorage;
import com.tencent.angel.ml.math2.storage.LongKeyVectorStorage;
import com.tencent.angel.ml.math2.storage.VectorStorage;
import com.tencent.angel.ml.math2.ufuncs.executor.BinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.DotExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.UnaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.*;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.commons.lang.NotImplementedException;

import java.io.Serializable;

public abstract class Vector extends MathObject implements Serializable {
  protected int rowId;

  protected VectorStorage storage;

  public void setStorage(VectorStorage storage) {
    this.storage = storage;
  }

  public VectorStorage getStorage() {
    return storage;
  }

  public void clear() {
    storage.clear();
  }

  public int getRowId() {
    return rowId;
  }

  public void setRowId(int rowId) {
    this.rowId = rowId;
  }

  public boolean isDense() {
    return storage.isDense();
  }

  public boolean isSparse() {
    return storage.isSparse();
  }

  public boolean isSorted() {
    return storage.isSorted();
  }

  public double dot(Vector other) {
    return DotExecutor.apply(this, other);
  }

  public Vector iadd(Vector other) {
    return BinaryExecutor.apply(this, other, new Add(true));
  }

  public Vector add(Vector other) {
    return BinaryExecutor.apply(this, other, new Add(false));
  }

  public Vector isub(Vector other) {
    return BinaryExecutor.apply(this, other, new Sub(true));
  }

  public Vector sub(Vector other) {
    return BinaryExecutor.apply(this, other, new Sub(false));
  }

  public Vector imul(Vector other) {
    return BinaryExecutor.apply(this, other, new Mul(true));
  }

  public Vector mul(Vector other) {
    return BinaryExecutor.apply(this, other, new Mul(false));
  }

  public Vector idiv(Vector other) {
    return BinaryExecutor.apply(this, other, new Div(true));
  }

  public Vector div(Vector other) {
    return BinaryExecutor.apply(this, other, new Div(false));
  }

  public Vector iaxpy(Vector other, double alpha) {
    return BinaryExecutor.apply(this, other, new Axpy(true, alpha));
  }

  public Vector axpy(Vector other, double alpha) {
    return BinaryExecutor.apply(this, other, new Axpy(false, alpha));
  }

  public Vector iadd(double x) {
    return UnaryExecutor.apply(this, new SAdd(true, x));
  }

  public Vector add(double x) {
    return UnaryExecutor.apply(this, new SAdd(false, x));
  }

  public Vector isub(double x) {
    return UnaryExecutor.apply(this, new SSub(true, x));
  }

  public Vector sub(double x) {
    return UnaryExecutor.apply(this, new SSub(false, x));
  }

  public Vector imul(double x) {
    return UnaryExecutor.apply(this, new SMul(true, x));
  }

  public Vector mul(double x) {
    return UnaryExecutor.apply(this, new SMul(false, x));
  }

  public Vector idiv(double x) {
    return UnaryExecutor.apply(this, new SDiv(true, x));
  }

  public Vector div(double x) {
    return UnaryExecutor.apply(this, new SDiv(false, x));
  }

  public abstract Vector filter(double threshold);

  public abstract Vector ifilter(double threshold);

  public abstract Vector filterUp(double threshold);

  public RowType getType() {
    if (null != storage) {
      return storage.getType();
    } else {
      return this.getType();
    }
  }

  public long getSize() {
    if (storage instanceof IntKeyVectorStorage) {
      return (long) ((IntKeyVectorStorage) storage).size();
    } else if (storage instanceof LongKeyVectorStorage) {
      return ((LongKeyVectorStorage) storage).size();
    } else if (this instanceof IntKeyVector) {
      return (long) ((IntKeyVector) this).size();
    } else if (this instanceof LongKeyVector) {
      return ((LongKeyVector) this).size();
    } else {
      throw new NotImplementedException();
    }
  }

  public abstract Vector copy();

  public abstract long dim();
}
