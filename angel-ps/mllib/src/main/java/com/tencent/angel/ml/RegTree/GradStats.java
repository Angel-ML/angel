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
package com.tencent.angel.ml.RegTree;

import com.tencent.angel.ml.param.RegTParam;

import java.util.List;


/**
 * Description: gradient info of reg tree
 */
public class GradStats {

  public float sumGrad; // sum of gradient statistics
  public float sumHess; // sum of hessian statistics

  // whether this is simply statistics, only need to call Add(gpair), instead of Add(gpair, info,
  // ridx)
  private int kSimpleStats = 1;

  public GradStats() {}

  public GradStats(GradStats old) {
    this(old.sumGrad, old.sumHess);
  }

  public GradStats(float sumGrad, float sumHess) {
    this.sumGrad = sumGrad;
    this.sumHess = sumHess;
  }

  private void clear() {
    this.sumGrad = 0.0f;
    this.sumHess = 0.0f;
  }

  public float getSumGrad() {
    return sumGrad;
  }

  public void setSumGrad(float sumGrad) {
    this.sumGrad = sumGrad;
  }

  public float getSumHess() {
    return sumHess;
  }

  public void setSumHess(float sumHess) {
    this.sumHess = sumHess;
  }

  // check if necessary information is ready
  public void CheckInfo(RegTDataStore info) {}

  public void update(float sumGrad, float sumHess) {
    this.sumGrad = sumGrad;
    this.sumHess = sumHess;
  }

  /**
   * add statistics to the data.
   *
   * @param grad the grad to add
   * @param hess the hess to add
   */
  public void add(float grad, float hess) {
    this.sumGrad += grad;
    this.sumHess += hess;
  }

  /**
   * accumulate statistics.
   *
   * @param p the gradient pair
   */
  public void add(GradPair p) {
    this.add(p.getGrad(), p.getHess());
  }

  /**
   * accumulate statistics, more complicated version.
   *
   * @param gpairs the list storing the gradient statistics
   * @param info the additional information
   * @param ridx instances index of this instances
   */
  public void add(List<GradPair> gpairs, RegTDataStore info, int ridx) {
    GradPair b = gpairs.get(ridx);
    this.add(b.getGrad(), b.getHess());
  }

  /**
   * add statistics to the data.
   *
   * @param b Grad statistic to add
   */
  public void add(GradStats b) {
    this.add(b.getSumGrad(), b.getSumHess());
  }

  // same as add, reduce is used in All Reduce
  public static void reduce(GradStats a, GradStats b) {
    a.add(b);
  }

  /**
   * set current value to a - b.
   *
   * @param a the grad being substracted
   * @param b the grad to substract
   */
  public void setSubstract(GradStats a, GradStats b) {
    sumGrad = a.getSumGrad() - b.getSumGrad();
    sumHess = a.getSumHess() - b.getSumHess();
  }

  /**
   * calculate leaf weight.
   *
   * @param param the param
   * @return the leaf weight
   */
  public float calcWeight(RegTParam param) {
    return param.calcWeight(sumGrad, sumHess);
  }


  /**
   * calculate gain of the solution.
   *
   * @param param the param
   * @return the loss gain
   */
  public float calcGain(RegTParam param) {
    return param.calcGain(sumGrad, sumHess);
  }


  /**
   * whether the statistics is not used yet .
   *
   * @return the boolean
   */
  public boolean empty() {
    return sumHess == 0.0f;
  }

  /**
   * set leaf vector value based on statistics .
   *
   * @param param the reg tree param
   * @param vec the leaf vector
   */
  public void setLeafVec(RegTParam param, float[] vec) {}

}
