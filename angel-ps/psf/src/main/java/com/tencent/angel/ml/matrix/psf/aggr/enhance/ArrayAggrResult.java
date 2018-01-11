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

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

/**
 * `ArrayAggrResult` is a result Class for all Aggregate function whose result is double[].
 */
public class ArrayAggrResult extends GetResult {
  private final long[] cols;
  private final double[] result;

  public ArrayAggrResult(double[] result) {
    this.result = result;
    this.cols = null;
  }

  public ArrayAggrResult(long[] cols, double[] result) {
    this.cols = cols;
    this.result = result;
  }

  public long[] getCols() {
    return cols;
  }

  public double[] getResult() {
    return result;
  }
}
