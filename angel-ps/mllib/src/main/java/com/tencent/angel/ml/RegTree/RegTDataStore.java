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

import com.tencent.angel.ml.math.vector.SparseDoubleSortedVector;
import com.tencent.angel.ml.param.FeatureMeta;
import com.tencent.angel.ml.param.RegTParam;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: data information, always sit in memory.
 */
public class RegTDataStore {

  public RegTParam param;

  public FeatureMeta featureMeta; // feature meta info

  public int numRow; // number of rows in the data
  public int numCol; // number of cols in the data
  public int numNonzero; // number of nonzero entries in the data
  public final static int kVersion = 1; // version flag, used to check version of this info

  public List<SparseDoubleSortedVector> instances; // training instances
  public float[] labels; // label of each instances
  public float[] preds; // pred of each instance
  public int[] rootIndex; // specified root index of each instances, can be used for multi task setting
  public int[] groupPtr; // the index of begin and end of a group, needed when the learning task is ranking.
  public float[] weights; // weights of each instances, optional

  public float[] baseWeights; // initial prediction to boost from.

  public RegTDataStore(RegTParam param) {
    this.param = param;
    this.instances = new ArrayList<SparseDoubleSortedVector>();
  }

  public void setFeatureMeta(FeatureMeta featureMeta) {
    this.featureMeta = featureMeta;
  }

  /**
   * Get weight of each instances.
   *
   * @param i the instances index
   * @return the weight
   */
  public float getWeight(int i) {
    return weights.length != 0 ? weights[i] : 1.0f;
  }

  public float getLabel(int i) {
    return labels.length != 0 ? labels[i] : 0.0f;
  }

  public float getBaseWeight(int i) {
    return baseWeights.length != 0 ? baseWeights[i] : 0.0f;
  }

  // clear all the information
  public void Clear() {}

  public void setNumRow(int numRow) {
    this.numRow = numRow;
  }

  public void setNumCol(int numCol) {
    this.numCol = numCol;
  }

  public void setNumNonzero(int numNonzero) {
    this.numNonzero = numNonzero;
  }

  public void setInstances(List<SparseDoubleSortedVector> instances) {
    this.instances.clear();
    this.instances.addAll(instances);
  }

  public void setLabels(float[] labels) {
    this.labels = labels;
  }

  public void setLables(int i, float label) {
    this.labels[i] = label;
  }

  public void setPreds(float[] preds) {
    this.preds = preds;
  }

  public void setWeights(float[] weights) {
    this.weights = weights;
  }

  public void setWeight(int i, float weight) {
    this.weights[i] = weight;
  }

  public void setBaseWeights(float[] baseWeights) {
    this.baseWeights = baseWeights;
  }

  public void setBaseWeight(int i, float baseWeight) {
    this.baseWeights[i] = baseWeight;
  }
}
