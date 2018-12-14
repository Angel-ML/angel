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


package com.tencent.angel.ml.feature;

import com.tencent.angel.ml.math2.vector.Vector;

/**
 * training data with label
 */
public class LabeledData {

  private Vector x;
  private double y;
  private String attached;

  public LabeledData(Vector x, double y) {
    this(x, y, null);
  }

  public LabeledData(Vector x, double y, String attached) {
    this.x = x;
    this.y = y;
    this.attached = attached;
  }

  public LabeledData() {
    this(null, 0);
  }

  public Vector getX() {
    return x;
  }

  public void setX(Vector x) {
    this.x = x;
  }

  public double getY() {
    return y;
  }

  public void setY(double y) {
    this.y = y;
  }

  public void attach(String msg) {
    attached = msg;
  }

  public String getAttach() {
    return attached;
  }
}
