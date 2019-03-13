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

package com.tencent.angel.spark.ml.tree.split;

import org.apache.spark.ml.linalg.Vector;

public class SplitPoint extends SplitEntry {

  private float fvalue;

  public SplitPoint() {
    this(-1, 0.0f, 0.0f);
  }

  public SplitPoint(int fid, float fvalue, float gain) {
    super(fid, gain);
    this.fvalue = fvalue;
  }

  @Override
  public int flowTo(float x) {
    return x < fvalue ? 0 : 1;
  }

  @Override
  public int flowTo(Vector x) {
    return x.apply(fid) < fvalue ? 0 : 1;
  }

  @Override
  public int defaultTo() {
    return fvalue > 0.0f ? 0 : 1;
  }

  public float getFvalue() {
    return fvalue;
  }

  public void setFvalue(float fvalue) {
    this.fvalue = fvalue;
  }

  @Override
  public SplitType splitType() {
    return SplitType.SPLIT_POINT;
  }

  @Override
  public String toString() {
    return String.format("%s fid[%d] fvalue[%f] gain[%f]",
        this.splitType(), fid, fvalue, gain);
  }
}
