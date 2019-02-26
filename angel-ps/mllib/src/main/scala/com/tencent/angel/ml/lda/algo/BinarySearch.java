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


package com.tencent.angel.ml.lda.algo;

public class BinarySearch {

  /* ! search the first x which x > u from [start,end] of p */
  public static int binarySearch(double[] p, double u, int start, int end) {
    int pstart = start, pend = end;

    while (pstart < pend) {
      if (pstart + 1 == pend) {
        if (p[pstart] > u)
          return pstart;
        else if (p[end] > u)
          return pend;
        else
          return -1;
      }

      int mid = (pstart + pend) / 2;
      double value = p[mid];
      if (value == u) {
        return mid + 1;
      }
      if (value < u) {
        pstart = mid + 1;
      } else {
        pend = mid;
      }
    }

    return pstart;
  }

  public static int binarySearch(float[] p, float u, int start, int end) {
    int pstart = start, pend = end;

    while (pstart < pend) {
      if (pstart + 1 == pend) {
        if (p[pstart] > u)
          return pstart;
        else if (p[end] > u)
          return pend;
        else
          return -1;
      }

      int mid = (pstart + pend) / 2;
      double value = p[mid];
      if (value == u) {
        return mid + 1;
      }
      if (value < u) {
        pstart = mid + 1;
      } else {
        pend = mid;
      }
    }

    return pstart;
  }
}