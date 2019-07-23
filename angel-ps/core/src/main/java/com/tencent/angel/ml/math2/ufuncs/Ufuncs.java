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


package com.tencent.angel.ml.math2.ufuncs;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.ufuncs.executor.BinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.DotExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.UnaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.BinaryMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.DotMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.UnaryMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.*;
import com.tencent.angel.ml.math2.vector.Vector;

public class Ufuncs {

  /*
  Computes v1 + v2
  */
  public static Vector add(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Add(false));
  }

  /*
  Computes v1 - v2
  */
  public static Vector sub(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Sub(false));
  }

  /*
  Computes v1 * v2
  */
  public static Vector mul(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Mul(false));
  }

  /*
  Computes v1 / v2
  */
  public static Vector div(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Div(false));
  }

  /*
  Computes v1 = v1 + v2
  */
  public static Vector iadd(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Add(true));
  }

  /*
  Computes v1 = v1 - v2
  */
  public static Vector isub(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Sub(true));
  }

  /*
  Computes v1 = v1 * v2
  */
  public static Vector imul(Vector v1, Vector v2) {
    throw new AngelException("The operation is not supported!");
  }

  /*
  Computes v1 = v1 / v2
  */
  public static Vector idiv(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Div(true));
  }

  /*
  Computes v1 + v2 * x
  */
  public static Vector axpy(Vector v1, Vector v2, double x) {
    return BinaryExecutor.apply(v1, v2, new Axpy(false, x));
  }

  /*
  Computes v1 = v1 + v2 * x
  */
  public static Vector iaxpy(Vector v1, Vector v2, double x) {
    return BinaryExecutor.apply(v1, v2, new Axpy(true, x));
  }

  public static Vector iaxpy2(Vector v1, Vector v2, double x) {
    return BinaryExecutor.apply(v1, v2, new Axpy2(true, x));
  }

  /*
 Compare v1 with v2, return the max value
 */
  public static Vector max(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Max(false));
  }

  public static Vector imax(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Max(true));
  }

  /*
  Compare v1 with v2, return the min value
  */
  public static Vector min(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Min(false));
  }

  public static Vector imin(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new Min(true));
  }

  public static Vector ftrlpossion(Vector v1, Vector v2, float p) {
    return BinaryExecutor.apply(v1, v2, new FtrlPossion(false, p));
  }

  /*
  Computes v1 + x
  */
  public static Vector sadd(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SAdd(false, x));
  }

  /*
  Computes v1 - x
  */
  public static Vector ssub(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SSub(false, x));
  }

  /*
  Computes v1 * x
  */
  public static Vector smul(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SMul(false, x));
  }

  /*
  Computes v1 / x
  */
  public static Vector sdiv(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SDiv(false, x));
  }

  /*
  Computes v1 = v1 + x
  */
  public static Vector isadd(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SAdd(true, x));
  }

  /*
  Computes v1 = v1 - x
  */
  public static Vector issub(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SSub(true, x));
  }

  /*
  Computes v1 = v1 * x
  */
  public static Vector ismul(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SMul(true, x));
  }

  /*
  Computes v1 = v1 / x
  */
  public static Vector isdiv(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SDiv(true, x));
  }

  /*
  Computes exp(v1)
  */
  public static Vector exp(Vector v1) {
    return UnaryExecutor.apply(v1, new Exp(false));
  }

  /*
  Computes log(v1)
  */
  public static Vector log(Vector v1) {
    return UnaryExecutor.apply(v1, new Log(false));
  }

  /*
  Computes log1p(v1)
  */
  public static Vector log1p(Vector v1) {
    return UnaryExecutor.apply(v1, new Log1p(false));
  }

  /*
  Computes pow(v1, x)
  */
  public static Vector pow(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new Pow(false, x));
  }

  /*
  Computes sqrt(v1)
  */
  public static Vector sqrt(Vector v1) {
    return UnaryExecutor.apply(v1, new Sqrt(false));
  }

  /*
  Computes sin(v1) * max(0, abs(v1) - x)
  */
  public static Vector softthreshold(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SoftThreshold(false, x));
  }

  public static Vector ftrlthreshold(Vector zv, Vector nv, double alpha, double beta,
      double lambda1, double lambda2) {
    return BinaryExecutor.apply(zv, nv, new FTRLThreshold(false, alpha, beta, lambda1, lambda2));
  }

  public static Vector ftrlthresholdinit(Vector zv, Vector nv, double alpha, double beta,
                                         double lambda1, double lambda2, double mean, double stdev) {
    return BinaryExecutor.apply(zv, nv, new FTRLThresholdInit(false, alpha, beta, lambda1, lambda2,
      mean, stdev));
  }

  public static Vector fmgrad(Vector x, Vector v, double dot) {
    return BinaryExecutor.apply(x, v, new FMGrad(false, dot));
  }

  public static Vector imomentumupdate(Vector v1, Vector v2, double momentum, double eta) {
    return BinaryExecutor.apply(v1, v2, new MomentUpdate(true, momentum, eta));
  }

  public static Vector indexget(Vector x, Vector y) {
    return BinaryExecutor.apply(x, y, new IndexGet(false));
  }

  /*
  Computes abs(v1)
  */
  public static Vector abs(Vector v1) {
    return UnaryExecutor.apply(v1, new Abs(false));
  }

  /*
  Computes - v1
  */
  public static Vector not(Vector v1) {
    return UnaryExecutor.apply(v1, new Not(false));
  }

  /*
  Computes for elem in v1 {
              if (elem > x){
                  return 1;
              }else {
                  return val;
              }
          }
  */
  public static Vector replace(Vector v1, double x, double val) {
    return UnaryExecutor.apply(v1, new Replace(false, x, val));
  }

  /*
  Computes v1 = exp(v1)
  */
  public static Vector iexp(Vector v1) {
    return UnaryExecutor.apply(v1, new Exp(true));
  }

  /*
  Computes v1 = log(v1)
  */
  public static Vector ilog(Vector v1) {
    return UnaryExecutor.apply(v1, new Log1p(true));
  }

  /*
  Computes v1 = log1p(v1)
  */
  public static Vector ilog1p(Vector v1) {
    return UnaryExecutor.apply(v1, new Log1p(true));
  }

  /*
  Computes v1 = pow(v1)
  */
  public static Vector ipow(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new Pow(true, x));
  }

  /*
  Computes v1 = sqrt(v1)
  */
  public static Vector isqrt(Vector v1) {
    return UnaryExecutor.apply(v1, new Sqrt(true));
  }

  /*
  Computes v1 = sin(v1) * max(0, abs(v1) - x)
  */
  public static Vector isoftthreshold(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new SoftThreshold(true, x));
  }

  /*
  Computes v1 = abs(v1)
  */
  public static Vector iabs(Vector v1) {
    return UnaryExecutor.apply(v1, new Abs(true));
  }

  /*
  Computes v1 = - v1
  */
  public static Vector inot(Vector v1) {
    return UnaryExecutor.apply(v1, new Not(true));
  }

  /*
  Computes for elem in v1 {
              if (elem > x){
                  return 1;
              }else {
                  return val;
              }
          }
          the result inplace the value of v1
  */
  public static Vector ireplace(Vector v1, double x, double val) {
    return UnaryExecutor.apply(v1, new Replace(true, x, val));
  }

  /*
  Computes m1 = m1 + m2
  */
  public static Matrix iadd(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Add(true));
  }

  /*
  Computes m1 + m2
  */
  public static Matrix add(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Add(false));
  }

  /*
  Computes m1 = m1 - m2
  */
  public static Matrix isub(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Sub(true));
  }

  /*
  Computes m1 - m2
  */
  public static Matrix sub(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Sub(false));
  }

  /*
  Computes m1 = m1 * m2
  */
  public static Matrix imul(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Mul(true));
  }

  /*
  Computes m1 * m2
  */
  public static Matrix mul(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Mul(false));
  }

  /*
  Computes m1 = m1 / m2
  */
  public static Matrix idiv(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Div(true));
  }

  /*
  Computes m1 / m2
  */
  public static Matrix div(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Div(false));
  }

  /*
  Computes m1 = m1 + m2 * x
  */
  public static Matrix iaxpy(Matrix m1, Matrix m2, double x) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Axpy(true, x));
  }

  /*
  Computes m1 + m2 * x
  */
  public static Matrix axpy(Matrix m1, Matrix m2, double x) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new Axpy(false, x));
  }

  /*
  Computes m1 = m1 + m2; (trans1: false, trans2: false)
           m1 = m1.T + m2; (trans1: true, trans2: false)
           m1 = m1 + m2.T; (trans1: false, trans2: true)
           m1 = m1.T + m2.T; (trans1: true, trans2: true)
  */
  public static Matrix iadd(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Add(true));
  }

  /*
  Computes m1 + m2; (trans1: false, trans2: false)
           m1.T + m2; (trans1: true, trans2: false)
           m1 + m2.T; (trans1: false, trans2: true)
           m1.T + m2.T; (trans1: true, trans2: true)
  */
  public static Matrix add(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Add(false));
  }

  /*
  Computes m1 = m1 - m2; (trans1: false, trans2: false)
           m1 = m1.T - m2; (trans1: true, trans2: false)
           m1 = m1 - m2.T; (trans1: false, trans2: true)
           m1 = m1.T - m2.T; (trans1: true, trans2: true)
  */
  public static Matrix isub(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Sub(true));
  }

  /*
  Computes m1 - m2; (trans1: false, trans2: false)
           m1.T - m2; (trans1: true, trans2: false)
           m1 - m2.T; (trans1: false, trans2: true)
           m1.T - m2.T; (trans1: true, trans2: true)
  */
  public static Matrix sub(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Sub(false));
  }

  /*
  Computes m1 = m1 * m2; (trans1: false, trans2: false)
           m1 = m1.T * m2; (trans1: true, trans2: false)
           m1 = m1 * m2.T; (trans1: false, trans2: true)
           m1 = m1.T * m2.T; (trans1: true, trans2: true)
  */
  public static Matrix imul(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Mul(true));
  }

  /*
  Computes m1 * m2; (trans1: false, trans2: false)
           m1.T * m2; (trans1: true, trans2: false)
           m1 * m2.T; (trans1: false, trans2: true)
           m1.T * m2.T; (trans1: true, trans2: true)
  */
  public static Matrix mul(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Mul(false));
  }

  /*
  Computes m1 = m1 / m2; (trans1: false, trans2: false)
           m1 = m1.T / m2; (trans1: true, trans2: false)
           m1 = m1 / m2.T; (trans1: false, trans2: true)
           m1 = m1.T / m2.T; (trans1: true, trans2: true)
  */
  public static Matrix idiv(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Div(true));
  }

  /*
  Computes m1 / m2; (trans1: false, trans2: false)
           m1.T / m2; (trans1: true, trans2: false)
           m1 / m2.T; (trans1: false, trans2: true)
           m1.T / m2.T; (trans1: true, trans2: true)
  */
  public static Matrix div(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Div(false));
  }

  /*
  Computes m1 = m1 + m2 * x; (trans1: false, trans2: false)
           m1 = m1.T * m2 * x; (trans1: true, trans2: false)
           m1 = m1 * m2.T * x; (trans1: false, trans2: true)
           m1 = m1.T * m2.T * x; (trans1: true, trans2: true)
  */
  public static Matrix iaxpy(Matrix m1, boolean trans1, Matrix m2, boolean trans2, double x) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Axpy(true, x));
  }

  /*
  Computes m1 + m2 * x; (trans1: false, trans2: false)
           m1.T * m2 * x; (trans1: true, trans2: false)
           m1 * m2.T * x; (trans1: false, trans2: true)
           m1.T * m2.T * x; (trans1: true, trans2: true)
  */
  public static Matrix axpy(Matrix m1, boolean trans1, Matrix m2, boolean trans2, double x) {
    return BinaryMatrixExecutor.apply(m1, trans1, m2, trans2, new Axpy(false, x));
  }

  /*
  Computes m + v; (trans: false, add on rows)
           m + v.T; (trans: true, add on cols)
  */
  public static Matrix add(Matrix m, Vector v, boolean trans) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Add(false));
  }

  /*
  Computes m = m + v; (trans: false, add on rows)
           m = m + v.T; (trans: true, add on cols)
  */
  public static Matrix iadd(Matrix m, Vector v, boolean trans) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Add(true));
  }

  /*
  Computes m - v; (trans: false, sub on rows)
           m - v.T; (trans: true, sub on cols)
  */
  public static Matrix sub(Matrix m, Vector v, boolean trans) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Sub(false));
  }

  /*
  Computes m = m - v; (trans: false, sub on rows)
           m = m - v.T; (trans: true, sub on cols)
  */
  public static Matrix isub(Matrix m, Vector v, boolean trans) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Sub(true));
  }

  /*
  Computes m * v; (trans: false, mul on rows)
           m * v.T; (trans: true, mul on cols)
  */
  public static Matrix mul(Matrix m, Vector v, boolean trans) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Mul(false));
  }

  /*
  Computes m = m * v; (trans: false, add on rows)
           m = m * v.T; (trans: true, add on cols)
  */
  public static Matrix imul(Matrix m, Vector v, boolean trans) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Mul(true));
  }

  /*
  Computes m / v; (trans: false, add on rows)
           m / v.T; (trans: true, add on cols)
  */
  public static Matrix div(Matrix m, Vector v, boolean trans) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Div(false));
  }

  /*
  Computes m = m / v; (trans: false, add on rows)
           m = m / v.T; (trans: true, add on cols)
  */
  public static Matrix idiv(Matrix m, Vector v, boolean trans) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Div(true));
  }

  /*
  Computes m + v * x; (trans: false, add on rows)
           m + v.T * x; (trans: true, add on cols)
  */
  public static Matrix axpy(Matrix m, Vector v, boolean trans, double x) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Axpy(false, x));
  }

  /*
  Computes m = m + v * x; (trans: false, add on rows)
           m = m + v.T * x; (trans: true, add on cols)
  */
  public static Matrix iaxpy(Matrix m, Vector v, boolean trans, double x) {
    return BinaryMatrixExecutor.apply(m, v, trans, new Axpy(true, x));
  }

  /*
  Computes m = m + x
  */
  public static Matrix isadd(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SAdd(true, x));
  }

  /*
  Computes m + x
  */
  public static Matrix sadd(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SAdd(false, x));
  }

  /*
  Computes m = m - x
  */
  public static Matrix issub(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SSub(true, x));
  }

  /*
  Computes m - x
  */
  public static Matrix ssub(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SSub(false, x));
  }

  /*
  Computes m = m * x
  */
  public static Matrix ismul(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SMul(true, x));
  }

  /*
  Computes m * x
  */
  public static Matrix smul(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SMul(false, x));
  }

  /*
  Computes m = m / x
  */
  public static Matrix isdiv(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SDiv(true, x));
  }

  /*
  Computes m / x
  */
  public static Matrix sdiv(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SDiv(false, x));
  }

  /*
  Computes m = exp(m)
  */
  public static Matrix iexp(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Exp(true));
  }

  /*
  Computes exp(m)
  */
  public static Matrix exp(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Exp(false));
  }

  /*
  Computes m = log(m)
  */
  public static Matrix ilog(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Log(true));
  }

  /*
  Computes log(m)
  */
  public static Matrix log(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Log(false));
  }

  /*
  Computes m = log1p(m)
  */
  public static Matrix ilog1p(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Log1p(true));
  }

  /*
  Computes log1p(m)
  */
  public static Matrix log1p(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Log1p(false));
  }

  /*
  Computes m = pow(m, x)
  */
  public static Matrix ipow(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new Pow(true, x));
  }

  /*
  Computes pow(m, x)
  */
  public static Matrix pow(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new Pow(false, x));
  }

  /*
  Computes - m
  */
  public static Matrix not(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Not(false));
  }

  /*
  Computes m = - m
  */
  public static Matrix inot(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Not(true));
  }

  /*
  Computes for elem in m {
              if (elem > x){
                  return 1;
              }else {
                  return val;
              }
          }
   */
  public static Matrix replace(Matrix m, double x, double val) {
    return UnaryMatrixExecutor.apply(m, new Replace(false, x, val));
  }

  /*
  Computes for elem in m {
             if (elem > x){
                 return 1;
             }else {
                 return val;
             }
         }
         the result inplace the value of m
  */
  public static Matrix ireplace(Matrix m, double x, double val) {
    return UnaryMatrixExecutor.apply(m, new Replace(true, x, val));
  }

  /*
  Computes m = sin(m) * max(0, abs(m) - x)
  */
  public static Matrix isoftthreshold(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SoftThreshold(true, x));
  }

  /*
  Computes sin(m) * max(0, abs(m) - x)
  */
  public static Matrix softthreshold(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SoftThreshold(false, x));
  }

  public static Matrix ftrlthreshold(Matrix zm, Matrix nm, double alpha, double beta,
      double lambda1, double lambda2) {
    return BinaryMatrixExecutor
        .apply(zm, false, nm, false, new FTRLThreshold(false, alpha, beta, lambda1, lambda2));
  }

  /*
  Computes m = sqrt(m)
  */
  public static Matrix isqrt(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Sqrt(true));
  }

  /*
  Computes sqrt(m)
  */
  public static Matrix sqrt(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Sqrt(false));
  }

  /*
  Computes v1 .* v2
  */
  public static double dot(Vector v1, Vector v2) {
    return DotExecutor.apply(v1, v2);
  }

  /*
  Computes m .* v
  */
  public static Vector dot(Matrix m, Vector v) {
    return DotMatrixExecutor.apply(m, false, v);
  }

  /*
  Computes m .* v; (trans: false)
           m.T .* v; (trans: true)
  */
  public static Vector dot(Matrix m, boolean trans, Vector v) {
    return DotMatrixExecutor.apply(m, trans, v);
  }

  /*
  Computes m1 .* m2
  */
  public static Matrix dot(Matrix m1, Matrix m2, Boolean parallel) {
    return DotMatrixExecutor.apply(m1, false, m2, false, parallel);
  }

  /*
  Computes m1 .* m2; (trans1: false, trans2: false)
           m1.T .* m2; (trans1: true, trans2: false)
           m1 .* m2.T; (trans1: false, trans2: true)
           m1.T .* m2.T; (trans1: true, trans2: true)
  */
  public static Matrix dot(Matrix m1, boolean trans1, Matrix m2, boolean trans2) {
    return DotMatrixExecutor.apply(m1, trans1, m2, trans2, true);
  }

  public static Matrix dot(Matrix m1, Matrix m2) {
    return DotMatrixExecutor.apply(m1, false, m2, false, true);
  }

  /*
  Computes m1 .* m2; (trans1: false, trans2: false)
           m1.T .* m2; (trans1: true, trans2: false)
           m1 .* m2.T; (trans1: false, trans2: true)
           m1.T .* m2.T; (trans1: true, trans2: true)
  */
  public static Matrix dot(Matrix m1, boolean trans1, Matrix m2, boolean trans2, Boolean parallel) {
    return DotMatrixExecutor.apply(m1, trans1, m2, trans2, parallel);
  }

  /*
  Computes x .* A .* x.T
  */
  public static double xAx(Matrix m, Vector v) {
    return DotMatrixExecutor.apply(m, v);
  }

  /*
  Computes x .* A .* y.T
  */
  public static double xAy(Matrix m, Vector v1, Vector v2) {
    return DotMatrixExecutor.apply(m, v1, v2);
  }

  /*
  Computes m + alpha * v1.T * v2
  */
  public static Matrix rank1update(Matrix m, double alpha, Vector v1, Vector v2) {
    return DotMatrixExecutor.apply(m, alpha, v1, v2);
  }
}