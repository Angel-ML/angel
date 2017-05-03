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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.lda.structures;

/**
 * Binary index tree only for trees whose size is a mutilpe of 2.
 */
public class FTree {

  public float[] tree;

  int length;

  public FTree(int length) {
    tree = new float[2 * length];
    this.length = length;
  }

  public FTree(float[] p, int length) {
    this(length);
    build(p);
  }

  public void build(float[] p) {
    for (int i = 2 * length - 1; i > 0; i--) {
      if (i >= length)
        tree[i] = p[i - length];
      else {
        tree[i] = tree[i << 1] + tree[(i << 1) + 1];
      }
    }
  }

  public void build() {
    for (int i = length - 1; i > 0; i--) {
      tree[i] = tree[i << 1] + tree[(i << 1) + 1];
    }
  }

  public void update(int index, float value) {
    int i = index + length;
    double delta = value - tree[i];
    while (i > 0) {
      tree[i] += delta;
      i >>= 1;
    }
  }

  public int sample(float u) {
    int i = 1;
    while (i < length) {
      if (u < tree[2 * i]) {
        i = i * 2;
      } else {
        u = u - tree[2 * i];
        i = i * 2 + 1;
      }
    }
    return i - length;
  }

  public float first() {
    return tree[1];
  }

  public float get(int index) {
    return tree[index + length];
  }

}
