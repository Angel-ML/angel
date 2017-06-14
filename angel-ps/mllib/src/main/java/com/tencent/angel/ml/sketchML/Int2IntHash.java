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

public class Int2IntHash {

  private int seed;
  private int length;

  public Int2IntHash(int seed, int length) {
    this.seed = seed;
    this.length = length;
  }

  public int encode(int input) {
    int ret = seed;
    while (input / 10 != 0) {
      ret = seed * ret + input % 10;
      input /= 10;
    }
    ret = ret % length;
    return ret >= 0 ? ret : ret + length;
  }

  public static void main(String[] argv) {
    int num = 10000;
    int[] inputs = new int[num];
    for (int i = 0; i < num; i++) {
      inputs[i] = i;
    }
    Int2IntHash hashFunc = new Int2IntHash(13, 100);
    int[] hashResults = new int[100];
    for (int input : inputs) {
      //System.out.println(input + " | " + hashFunc.encode(input));
      hashResults[hashFunc.encode(input)] += 1;
    }
    int item = 0;
    int freq = 0;
    for (int i = 0; i < hashResults.length; i++) {
      if (hashResults[i] != 0) {
        System.out.println("Item: " + i + ", freq: " + hashResults[i]);
        item++;
        freq += hashResults[i];
      }
    }
    System.out.println("Item: " + item + ", average collision: " + (double) (freq / item));
  }
}
