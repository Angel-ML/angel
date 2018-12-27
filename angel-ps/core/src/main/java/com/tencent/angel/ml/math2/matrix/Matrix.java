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


package com.tencent.angel.ml.math2.matrix;

import com.tencent.angel.ml.math2.MathObject;
import com.tencent.angel.ml.math2.vector.Vector;
import org.apache.commons.lang.NotImplementedException;
import org.omg.CORBA.PUBLIC_MEMBER;

public abstract class Matrix extends MathObject {

  public abstract Vector diag();

  public abstract Vector getRow(int idx);

  public abstract Vector getCol(int idx);

  public abstract Vector dot(Vector other);

  public abstract Vector transDot(Vector other);

  public Matrix dot(Matrix other, Boolean parallel) {
    throw new NotImplementedException();
  }

  public abstract Matrix iadd(int rowId, Vector other);

  public abstract Matrix add(int rowId, Vector other);

  public abstract Matrix isub(int rowId, Vector other);

  public abstract Matrix sub(int rowId, Vector other);

  public abstract Matrix imul(int rowId, Vector other);

  public abstract Matrix mul(int rowId, Vector other);

  public abstract Matrix idiv(int rowId, Vector other);

  public abstract Matrix div(int rowId, Vector other);

  public abstract Matrix iaxpy(int rowId, Vector other, double aplha);

  public abstract Matrix axpy(int rowId, Vector other, double aplha);

  public abstract Matrix iadd(Vector other);

  public abstract Matrix add(Vector other);

  public abstract Matrix isub(Vector other);

  public abstract Matrix sub(Vector other);

  public abstract Matrix imul(Vector other);

  public abstract Matrix mul(Vector other);

  public abstract Matrix idiv(Vector other);

  public abstract Matrix div(Vector other);

  public abstract Matrix iaxpy(Vector other, double aplha);

  public abstract Matrix axpy(Vector other, double aplha);

  public abstract Matrix iadd(Matrix other);

  public abstract Matrix add(Matrix other);

  public abstract Matrix isub(Matrix other);

  public abstract Matrix sub(Matrix other);

  public abstract Matrix imul(Matrix other);

  public abstract Matrix mul(Matrix other);

  public abstract Matrix idiv(Matrix other);

  public abstract Matrix div(Matrix other);

  public abstract Matrix iaxpy(Matrix other, double aplha);

  public abstract Matrix axpy(Matrix other, double aplha);

  public abstract Matrix iadd(double x);

  public abstract Matrix add(double x);

  public abstract Matrix isub(double x);

  public abstract Matrix sub(double x);

  public abstract Matrix imul(double x);

  public abstract Matrix mul(double x);

  public abstract Matrix idiv(double x);

  public abstract Matrix div(double x);

  public abstract double min();

  public abstract double max();

  public abstract Vector min(int axis);

  public abstract Vector max(int axis);

  public abstract Vector sum(int axis);

  public abstract Vector average(int axis);

  public abstract Vector std(int axis);

  public abstract Vector norm(int axis);

  public abstract Matrix copy();

  public abstract int getNumRows();
}