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

  public CMSketch(int length) {
    h = new Int2IntHash[3];
    h[0] = new Int2IntHash(13, length);
    h[1] = new Int2IntHash(131, length);
    h[2] = new Int2IntHash(1313, length);
    t = new int[3][];
    t[0] = new int[length];
    t[1] = new int[length];
    t[2] = new int[length];
  }

  public void insert(int key, int freq) {
    for (int i = 0; i < 3; i++) {
      int code = h[i].encode(key);
      System.out.println(code);
      t[i][code] += freq;
    }
  }

  public int get(int key) {
    int ret = Integer.MAX_VALUE;
    for (int i = 0; i < 3; i++) {
      ret = Math.min(ret, t[i][h[i].encode(key)]);
    }
    return ret;
  }

  public static void main(String[] argv) {
    CMSketch sk = new CMSketch(100);
    int key = 123;
    int freq = 3;
    sk.insert(key, freq);
    System.out.println(sk.get(key));
  }

}
