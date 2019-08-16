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
package com.tencent.angel.spark.ml.psf.pagerank;

import com.tencent.angel.ml.math2.ufuncs.expression.Unary;

public class LessZero extends Unary {

  private float threshold;

  public LessZero(boolean inplace, float threshold) {
    setInplace(inplace);
    this.threshold = threshold;
  }

  @Override
  public double apply(double elem) {
    if (elem < threshold) return 0;
    return elem;
  }

  @Override
  public float apply(float elem) {
    if (elem < threshold) return 0;
    return elem;
  }

  @Override
  public long apply(long elem) {
    if (elem < threshold) return 0;
    return elem;
  }

  @Override
  public int apply(int elem) {
    if (elem < threshold) return 0;
    return elem;
  }
}
