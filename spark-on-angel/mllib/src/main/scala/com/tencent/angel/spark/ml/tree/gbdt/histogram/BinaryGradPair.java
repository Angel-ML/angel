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


package com.tencent.angel.spark.ml.tree.gbdt.histogram;

import com.tencent.angel.spark.ml.tree.tree.param.GBDTParam;
import java.io.Serializable;

public class BinaryGradPair implements GradPair, Serializable {

  private double grad;
  private double hess;

  public BinaryGradPair() {
  }

  public BinaryGradPair(double grad, double hess) {
    this.grad = grad;
    this.hess = hess;
  }

  @Override
  public void plusBy(GradPair gradPair) {
    this.grad += ((BinaryGradPair) gradPair).grad;
    this.hess += ((BinaryGradPair) gradPair).hess;
  }

  public void plusBy(double grad, double hess) {
    this.grad += grad;
    this.hess += hess;
  }

  @Override
  public void subtractBy(GradPair gradPair) {
    this.grad -= ((BinaryGradPair) gradPair).grad;
    this.hess -= ((BinaryGradPair) gradPair).hess;
  }

  public void subtractBy(double grad, double hess) {
    this.grad -= grad;
    this.hess -= hess;
  }

  @Override
  public GradPair plus(GradPair gradPair) {
    GradPair res = this.copy();
    res.plusBy(gradPair);
    return res;
  }

  public GradPair plus(double grad, double hess) {
    return new BinaryGradPair(this.grad + grad, this.hess + hess);
  }

  @Override
  public GradPair subtract(GradPair gradPair) {
    GradPair res = this.copy();
    res.subtractBy(gradPair);
    return res;
  }

  public GradPair subtract(double grad, double hess) {
    return new BinaryGradPair(this.grad - grad, this.hess - hess);
  }

  @Override
  public void timesBy(double x) {
    this.grad *= x;
    this.hess *= x;
  }

  @Override
  public float calcGain(GBDTParam param) {
    return (float) param.calcGain(grad, hess);
  }

  public float calcWeight(GBDTParam param) {
    return (float) param.calcWeight(grad, hess);
  }

  @Override
  public boolean satisfyWeight(GBDTParam param) {
    return param.satisfyWeight(hess);
  }

  @Override
  public GradPair copy() {
    return new BinaryGradPair(grad, hess);
  }

  public double getGrad() {
    return grad;
  }

  public double getHess() {
    return hess;
  }

  public void setGrad(double grad) {
    this.grad = grad;
  }

  public void setHess(double hess) {
    this.hess = hess;
  }

  public void set(double grad, double hess) {
    this.grad = grad;
    this.hess = hess;
  }

  @Override
  public String toString() {
    return "(" + grad + ", " + hess + ")";
  }
}
