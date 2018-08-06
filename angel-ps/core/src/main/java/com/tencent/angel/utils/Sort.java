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

package com.tencent.angel.utils;

import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import it.unimi.dsi.fastutil.ints.IntComparator;

/**
 * Quick sort utils
 */
public class Sort {

  public static void quickSort(int[] array, double[] values, int low, int high) {
    if (low < high) {
      int tmp = array[low];
      double tmpValue = values[low];
      int ii = low, jj = high;
      while (ii < jj) {
        while (ii < jj && array[jj] >= tmp) {
          jj--;
        }

        array[ii] = array[jj];
        values[ii] = values[jj];

        while (ii < jj && array[ii] <= tmp) {
          ii++;
        }

        array[jj] = array[ii];
        values[jj] = values[ii];
      }
      array[ii] = tmp;
      values[ii] = tmpValue;

      quickSort(array, values, low, ii - 1);
      quickSort(array, values, ii + 1, high);
    }
  }

  public static void quickSort(long[] array, double[] values, int low, int high) {
    if (low < high) {
      long tmp = array[low];
      double tmpValue = values[low];
      int ii = low, jj = high;
      while (ii < jj) {
        while (ii < jj && array[jj] >= tmp) {
          jj--;
        }

        array[ii] = array[jj];
        values[ii] = values[jj];

        while (ii < jj && array[ii] <= tmp) {
          ii++;
        }

        array[jj] = array[ii];
        values[jj] = values[ii];
      }
      array[ii] = tmp;
      values[ii] = tmpValue;

      quickSort(array, values, low, ii - 1);
      quickSort(array, values, ii + 1, high);
    }
  }

  public static void quickSort(long[] array, int low, int high) {
    if (low < high) {
      long tmp = array[low];
      int ii = low, jj = high;
      while (ii < jj) {
        while (ii < jj && array[jj] >= tmp) {
          jj--;
        }

        array[ii] = array[jj];

        while (ii < jj && array[ii] <= tmp) {
          ii++;
        }

        array[jj] = array[ii];
      }
      array[ii] = tmp;

      quickSort(array, low, ii - 1);
      quickSort(array,  ii + 1, high);
    }
  }

  public static void quickSort(int[] array, int low, int high) {
    if (low < high) {
      int tmp = array[low];
      int ii = low, jj = high;
      while (ii < jj) {
        while (ii < jj && array[jj] >= tmp) {
          jj--;
        }

        array[ii] = array[jj];

        while (ii < jj && array[ii] <= tmp) {
          ii++;
        }

        array[jj] = array[ii];
      }
      array[ii] = tmp;

      quickSort(array, low, ii - 1);
      quickSort(array,  ii + 1, high);
    }
  }

  public static void quickSort(int[] array, int[] values, int low, int high) {
    if (low < high) {
      int tmp = array[low];
      int tmpValue = values[low];
      int ii = low, jj = high;
      while (ii < jj) {
        while (ii < jj && array[jj] >= tmp) {
          jj--;
        }

        array[ii] = array[jj];
        values[ii] = values[jj];

        while (ii < jj && array[ii] <= tmp) {
          ii++;
        }

        array[jj] = array[ii];
        values[jj] = values[ii];
      }
      array[ii] = tmp;
      values[ii] = tmpValue;

      quickSort(array, values, low, ii - 1);
      quickSort(array, values, ii + 1, high);
    }
  }

  public static void quickSort(double[] x, double[] y, int from, int to, DoubleComparator comp) {
    int len = to - from;
    if (len < 7) {
      selectionSort(x, y, from, to, comp);
    } else {
      int m = from + len / 2;
      int v;
      int a;
      int b;
      if (len > 7) {
        v = from;
        a = to - 1;
        if (len > 50) {
          b = len / 8;
          v = med3(x, from, from + b, from + 2 * b, comp);
          m = med3(x, m - b, m, m + b, comp);
          a = med3(x, a - 2 * b, a - b, a, comp);
        }

        m = med3(x, v, m, a, comp);
      }

      double seed = x[m];
      a = from;
      b = from;
      int c = to - 1;
      int d = c;

      while (true) {
        int s;
        while (b > c || (s = comp.compare(x[b], seed)) > 0) {
          for (; c >= b && (s = comp.compare(x[c], seed)) >= 0; --c) {
            if (s == 0) {
              swap(x, c, d);
              swap(y, c, d);
              d--;
            }
          }

          if (b > c) {
            s = Math.min(a - from, b - a);
            vecSwap(x, from, b - s, s);
            vecSwap(y, from, b - s, s);
            s = Math.min(d - c, to - d - 1);
            vecSwap(x, b, to - s, s);
            vecSwap(y, b, to - s, s);
            if ((s = b - a) > 1) {
              quickSort(x, y, from, from + s, comp);
            }

            if ((s = d - c) > 1) {
              quickSort(x, y, to - s, to, comp);
            }

            return;
          }

          swap(x, b, c);
          swap(y, b, c);
          b++;
          c--;
        }

        if (s == 0) {
          swap(x, a, b);
          swap(y, a, b);
          a++;
        }

        ++b;
      }
    }
  }

  public static void quickSort(int[] array, float[] values, int low, int high) {
    if (low < high) {
      int tmp = array[low];
      float tmpValue = values[low];
      int ii = low, jj = high;
      while (ii < jj) {
        while (ii < jj && array[jj] >= tmp) {
          jj--;
        }

        array[ii] = array[jj];
        values[ii] = values[jj];

        while (ii < jj && array[ii] <= tmp) {
          ii++;
        }

        array[jj] = array[ii];
        values[jj] = values[ii];
      }
      array[ii] = tmp;
      values[ii] = tmpValue;

      quickSort(array, values, low, ii - 1);
      quickSort(array, values, ii + 1, high);
    }
  }


  private static int med3(double[] x, int a, int b, int c, DoubleComparator comp) {
    int ab = comp.compare(x[a], x[b]);
    int ac = comp.compare(x[a], x[c]);
    int bc = comp.compare(x[b], x[c]);
    return ab < 0 ? (bc < 0 ? b : (ac < 0 ? c : a)) : (bc > 0 ? b : (ac > 0 ? c : a));
  }

  private static void vecSwap(double[] x, int a, int b, int n) {
    for (int i = 0; i < n; ++b) {
      swap(x, a, b);
      ++i;
      ++a;
    }

  }

  private static void swap(double[] x, int a, int b) {
    double t = x[a];
    x[a] = x[b];
    x[b] = t;
  }

  public static void selectionSort(int[] a, int[] y, int from, int to, IntComparator comp) {
    for (int i = from; i < to - 1; ++i) {
      int m = i;

      int u;
      for (u = i + 1; u < to; ++u) {
        if (comp.compare(a[u], a[m]) < 0) {
          m = u;
        }
      }

      if (m != i) {
        u = a[i];
        a[i] = a[m];
        a[m] = u;
        u = y[i];
        y[i] = y[m];
        y[m] = u;
      }
    }

  }

  public static void selectionSort(double[] a, double[] y, int from, int to,
      DoubleComparator comp) {
    for (int i = from; i < to - 1; ++i) {
      int m = i;
      for (int u = i + 1; u < to; ++u) {
        if (comp.compare(a[u], a[m]) < 0) {
          m = u;
        }
      }

      if (m != i) {
        double temp = a[i];
        a[i] = a[m];
        a[m] = temp;
        temp = y[i];
        y[i] = y[m];
        y[m] = temp;
      }
    }
  }
}
