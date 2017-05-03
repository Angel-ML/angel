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

package com.tencent.angel.ml.feature;

public class TrainSample {
  private double target;
  private final long[] colmunValues;

  public TrainSample(int length) {
    colmunValues = new long[length];
    setTarget(0);
  }

  public TrainSample(double y, long[] values) {
    target = y;
    colmunValues = values;
  }

  public void setColumnValue(int index, int value) {
    colmunValues[index] = value;
  }

  public void setColumnValue(int index, long value) {
    colmunValues[index] = value;
  }

  public void setColumnValue(int index, float value) {
    colmunValues[index] = (long) value;
  }

  public void setColumnValue(int index, double value) {
    colmunValues[index] = (long) value;
  }

  public void setColumnValue(int index, String value) {
    colmunValues[index] = Long.valueOf(value);
  }

  public long[] getValues() {
    return colmunValues;
  }

  public double getTarget() {
    return target;
  }

  public void setTarget(double target) {
    this.target = target;
  }
}
