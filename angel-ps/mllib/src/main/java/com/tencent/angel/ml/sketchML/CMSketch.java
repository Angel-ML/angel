/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.sketchML;

public class CMSketch {

  private Int2IntHash[] h;
  private int[][] t;
  private int zeroIdx = 0;

  public CMSketch(int length) {
    h = new Int2IntHash[3];
    h[0] = new Mix64Hash(length);
    h[1] = new TWHash(length);
    h[2] = new BJHash(length);
    t = new int[3][];
    t[0] = new int[length];
    t[1] = new int[length];
    t[2] = new int[length];
  }

  public void setZeroIdx(int idx) {
    this.zeroIdx = idx;
  }

  public void insert(int key, int freq) {
    for (int i = 0; i < 3; i++) {
      int code = h[i].encode(key);
      //t[i][code]++;
      if (t[i][code] == 0) {
        t[i][code] = freq;
      }
      //else if (Math.random() > 0.5) {
      //  t[i][code] = freq;
      //}
      else if (Math.abs(t[i][code] - zeroIdx) > Math.abs(freq - zeroIdx)
        && (t[i][code] - zeroIdx) * (freq - zeroIdx) > 0) {
        //System.out.println("Change from " + t[i][code] + " to " + freq);
        t[i][code] = freq;
      }
    }
  }

  public int get(int key) {
    int ret = Integer.MAX_VALUE;
    for (int i = 0; i < 3; i++) {
      int curIdx = t[i][h[i].encode(key)];
      if (Math.abs(curIdx - zeroIdx) < Math.abs(ret - zeroIdx)) {
        ret = curIdx;
      }
      //ret = Math.min(ret, t[i][h[i].encode(key)]);
    }
    return ret;
  }

  public int[] getTable(int i) {
    return t[i];
  }

  public void distribution() {
    for (int i = 0; i < 3; i++) {
      double mean = mean(t[i]);
      double var = var(t[i], mean);
      System.out.println("Hash table: " + i + ", mean:" + mean + ", variance: " + var);
    }
  }

  public double mean(int[] arr) {
    double ret = 0.0;
    for (int item : arr) {
      ret += item;
    }
    return ret / arr.length;
  }

  public double var(int[] arr, double mean) {
    double ret = 0.0;
    for (int item : arr) {
      ret += Math.pow(item - mean, 2);
    }
    return Math.sqrt(ret / arr.length);
  }

}
