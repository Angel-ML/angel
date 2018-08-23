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

import org.junit.BeforeClass;
import org.junit.Test;
import com.github.fommil.netlib.BLAS;

// https://blog.csdn.net/cocoonyang/article/details/63068108
public class NetLib {
  private static double[] v0, v1, v2;
  private static BLAS blas;
  private static int n;

  private double axpy(double x, double y, double alpha) {
    return x + alpha * y;
  }

  @BeforeClass public static void init() {
    blas = BLAS.getInstance();

    n = 1000000;

    v0 = new double[n];
    v1 = new double[n];
    v2 = new double[n];
    for (int i = 0; i < n; i++) {
      v1[i] = Math.random();
      v2[i] = Math.random();
    }
  }

  @Test public void testVectorDot() {
    long start, stop, t1 = 0L, t2 = 0L;

    for (int k = 0; k < 1000; k++) {
      start = System.currentTimeMillis();
      double res1 = blas.ddot(n, v1, 0, 1, v2, 0, 1);
      stop = System.currentTimeMillis();
      t1 += stop - start;

      start = System.currentTimeMillis();
      double res2 = 0.0;
      for (int i = 0; i < n; i++) {
        res2 += v1[i] * v2[i];
      }
      stop = System.currentTimeMillis();
      t2 += stop - start;
    }

    System.out.println(t1);
    System.out.println(t2);
  }

  @Test public void testVectorAxpy() {
    long start, stop, t1 = 0L, t2 = 0L;
    double da = 0.65;

    for (int k = 0; k < 1000; k++) {
      start = System.currentTimeMillis();
      blas.daxpy(n, da, v1, 0, 1, v2, 0, 1);
      stop = System.currentTimeMillis();
      t1 += stop - start;

      start = System.currentTimeMillis();
      for (int i = 0; i < n; i++) {
        // v1[i] = v1[i] + da * v2[i];
        v1[i] = axpy(v1[i], v2[i], da);
      }
      stop = System.currentTimeMillis();
      t2 += stop - start;
    }

    System.out.println(t1);
    System.out.println(t2);
  }

  @Test public void testMMDot() {
    long start, stop, t1 = 0L, t2 = 0L;
    int r = 1000, c = 1000;

    for (int t = 0; t < 10; t++) {
      start = System.currentTimeMillis();
      // dgemm(String transa, String transb,
      //       int m, int n, int k,
      //       double alpha,
      //       double[] a, int lda,
      //       double[] b, int ldb,
      //       double beta,
      //       double[] c, int ldc);

      // C := alpha*op( A )*op( B ) + beta*C
      blas.dgemm("N", "N", r, c, c, 1.0, v1, r, v2, c, 0.0, v0, r);
      stop = System.currentTimeMillis();
      t1 += stop - start;

      start = System.currentTimeMillis();
      for (int i = 0; i < r; i++) {
        for (int j = 0; j < c; j++) {
          for (int k = 0; k < c; k++) {
            // v1[i] = v1[i] + da * v2[i];
            v0[i * r + j] += v1[i * r + k] * v2[k * r + j];
          }
        }
      }
      stop = System.currentTimeMillis();
      t2 += stop - start;
    }

    System.out.println(t1);
    System.out.println(t2);
  }

  @Test public void testMMDot1() {
    int m = 4, k = 5, n = 6;

    double[] m1 = new double[m * k];
    for (int i = 0; i < m * k; i++) {
      m1[i] = Math.random();
    }
    double[] m2 = new double[k * n];
    for (int i = 0; i < k * n; i++) {
      m2[i] = Math.random();
    }

    double[] resBlas = dotBlas(m1, m, k, false, m2, k, n, false);

    double[] resJava = new double[n * m];
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        for (int t = 0; t < k; t++) {
          resJava[i * n + j] += m1[i * k + t] * m2[t * n + j];
        }
      }
    }

    for (int i = 0; i < m * n; i++) {
      System.out.println(Math.abs(resBlas[i] - resJava[i]));
    }

  }

  @Test public void testMMDot2() {
    int m = 4, n = 6, p = 8, q = 6;

    double[] m1 = new double[m * n];
    for (int i = 0; i < m * n; i++) {
      m1[i] = Math.random();
    }
    double[] m2 = new double[p * q];
    for (int i = 0; i < p * q; i++) {
      m2[i] = Math.random();
    }

    double[] resBlas = dotBlas(m1, m, n, false, m2, p, q, true);

    double[] resJava = new double[m * p];
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < p; j++) {
        for (int t = 0; t < n; t++) {
          resJava[i * p + j] += m1[i * n + t] * m2[j * q + t];
        }
      }
    }

    for (int i = 0; i < m * p; i++) {
      System.out.println(Math.abs(resBlas[i] - resJava[i]));
    }

  }

  @Test public void testMMDot3() {
    int m = 6, n = 4, p = 6, q = 8;

    double[] m1 = new double[m * n];
    for (int i = 0; i < m * n; i++) {
      m1[i] = Math.random();
    }
    double[] m2 = new double[p * q];
    for (int i = 0; i < p * q; i++) {
      m2[i] = Math.random();
    }

    double[] resBlas = dotBlas(m1, m, n, true, m2, p, q, false);

    double[] resJava = new double[n * q];
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < q; j++) {
        for (int t = 0; t < m; t++) {
          resJava[i * q + j] += m1[t * n + i] * m2[t * q + j];
        }
      }
    }

    for (int i = 0; i < n * q; i++) {
      System.out.println(Math.abs(resBlas[i] - resJava[i]));
    }

  }

  @Test public void testMMDot4() {
    int m = 6, n = 4, p = 8, q = 6;

    double[] m1 = new double[m * n];
    for (int i = 0; i < m * n; i++) {
      m1[i] = Math.random();
    }
    double[] m2 = new double[p * q];
    for (int i = 0; i < p * q; i++) {
      m2[i] = Math.random();
    }

    double[] resBlas = dotBlas(m1, m, n, true, m2, p, q, true);

    double[] resJava = new double[n * p];
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < p; j++) {
        for (int t = 0; t < m; t++) {
          resJava[i * p + j] += m1[t * n + i] * m2[j * q + t];
        }
      }
    }

    for (int i = 0; i < n * p; i++) {
      System.out.println(Math.abs(resBlas[i] - resJava[i]));
    }

  }

  @Test public void testMVdot() {
    long start, stop, t1 = 0L, t2 = 0L;
    int r = 1000, c = 1000;

    double[] v = new double[r];
    for (int i = 0; i < r; i++) {
      v[i] = Math.random();
    }

    double[] res = new double[r];

    // y := alpha*A*x + beta*y
    for (int t = 0; t < 1000; t++) {
      start = System.currentTimeMillis();
      blas.dgemv("N", r, c, 1.0, v1, r, v, 1, 0.0, res, 1);
      stop = System.currentTimeMillis();
      t1 += stop - start;

      start = System.currentTimeMillis();
      for (int i = 0; i < r; i++) {
        for (int j = 0; j < c; j++) {
          res[i] += v1[i * r + j] * v[j];
        }
      }
      stop = System.currentTimeMillis();
      t2 += stop - start;
    }

    System.out.println(t1);
    System.out.println(t2);
  }

  @Test public void testMVdot1() {
    int r = 4, c = 5;

    double[] m = new double[r * c];
    for (int i = 0; i < r * c; i++) {
      m[i] = Math.random();
    }

    double[] v = new double[c];
    for (int i = 0; i < c; i++) {
      v[i] = Math.random();
    }

    double[] resBlas = new double[r];
    double[] resJava = new double[r];

    // y := alpha*A*x + beta*y
    blas.dgemv("T", c, r, 1.0, m, c, v, 1, 0.0, resBlas, 1);

    for (int i = 0; i < r; i++) {
      for (int j = 0; j < c; j++) {
        resJava[i] += m[i * c + j] * v[j];
      }
    }

    for (int i = 0; i < r; i++) {
      System.out.println(Math.abs(resBlas[i] - resJava[i]));
    }
  }

  @Test public void testMVdot2() {
    int r = 4, c = 5;

    double[] m = new double[r * c];
    for (int i = 0; i < r * c; i++) {
      m[i] = Math.random();
    }

    double[] v = new double[r];
    for (int i = 0; i < r; i++) {
      v[i] = Math.random();
    }

    double[] resBlas = new double[c];
    double[] resJava = new double[c];

    // y := alpha*A*x + beta*y
    blas.dgemv("N", c, r, 1.0, m, c, v, 1, 0.0, resBlas, 1);

    for (int i = 0; i < r; i++) {
      for (int j = 0; j < c; j++) {
        resJava[j] += m[i * c + j] * v[i];
      }
    }

    for (int i = 0; i < c; i++) {
      System.out.println(Math.abs(resBlas[i] - resJava[i]));
    }
  }

  private double[] dotBlas(double[] m1, int m, int n, boolean trans1, double[] m2, int p, int q,
    boolean trans2) {
    // dgemm(String transa, String transb,
    //       int m, int n, int k,
    //       double alpha,
    //       double[] a, int lda,
    //       double[] b, int ldb,
    //       double beta,
    //       double[] c, int ldc);

    // C := alpha*op( A )*op( B ) + beta*C

    double alpha = 1.0, beta = 0.0;
    double[] resBlas;

    if (trans1 && trans2) { // M1^T * M2^T
      assert m == q;
      resBlas = new double[n * p];

      blas.dgemm("T", "T", p, n, m, alpha, m2, q, m1, n, beta, resBlas, p);

      return resBlas;
    } else if (!trans1 && trans2) { // M1 * M2^T
      assert n == q;
      resBlas = new double[m * p];

      blas.dgemm("T", "N", p, m, n, alpha, m2, q, m1, n, beta, resBlas, p);

      return resBlas;
    } else if (trans1 && !trans2) { // M1^T * M2
      assert m == p;
      resBlas = new double[n * q];

      blas.dgemm("N", "T", q, n, m, alpha, m2, q, m1, n, beta, resBlas, q);

      return resBlas;
    } else { // M1 * M2
      assert n == p;
      resBlas = new double[m * q];

      blas.dgemm("N", "N", q, m, n, alpha, m2, q, m1, n, beta, resBlas, q);

      return resBlas;
    }
  }
}
