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
package com.tencent.angel.graph.client.node2vec;

public class PartitionHasher {
  public static int getHash(long e, int mod) {
    int rawMod = (int) (e % mod);
    if (rawMod < 0) {
      return rawMod + mod;
    } else {
      return rawMod;
    }
  }

  public static int getHash(long e1, long e2, int mod) {
    int rawMod;
    if (e1 < e2) {
      rawMod = (int) (((e1 >> 1) + e2) % mod);
    } else {
      rawMod = (int) (((e2 >> 1) + e1) % mod);
    }

    if (rawMod < 0) {
      return rawMod + mod;
    } else {
      return rawMod;
    }
  }

  public static int getHash(int e, int mod) {
    int rawMod = e % mod;
    if (rawMod < 0) {
      return rawMod + mod;
    } else {
      return rawMod;
    }
  }

  public static int getHash(int e1, int e2, int mod) {
    int rawMod;
    if (e1 < e2) {
      rawMod = (int) ((((long) e1 << 32) + e2) % mod);
    } else {
      rawMod = (int) ((((long) e2 << 32) + e1) % mod);
    }

    if (rawMod < 0) {
      return rawMod + mod;
    } else {
      return rawMod;
    }
  }
}
