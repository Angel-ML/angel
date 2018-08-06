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

package com.tencent.angel.ml.sketchML;

public class MulHash extends Int2IntHash {

  private int seed;

  public MulHash(int length) {
    super(length);
  }

  public MulHash(int seed, int length) {
    this(length);
    this.seed = seed;
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

}
