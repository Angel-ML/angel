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

import com.tencent.angel.ml.math.TAbstractVector;

/**
 * training data with label
 *
 */
public class LabeledData {

  private TAbstractVector x;
  private double y;
  private double y1;

  private double score;

  /**
   * @param x
   * @param y
   */
  public LabeledData(TAbstractVector x, double y) {
    super();
    this.x = x;
    this.y = y;
  }

  public LabeledData() {
    super();
    x = null;
    y = 0;
  }

  public TAbstractVector getX() {
    return x;
  }

  public void setX(TAbstractVector x) {
    this.x = x;
  }

  public double getY() {
    return y;
  }

  public void setY(double y) {
    this.y = y;
  }

  public void setY1(double y1) {
    this.y1 = y1;
  }

  public double getY1() {
    return y1;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }
}
