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


package com.tencent.angel.ml.GBDT.algo.RegTree;

import com.tencent.angel.ml.GBDT.algo.FeatureMeta;
import com.tencent.angel.ml.math2.utils.LabeledData;
import com.tencent.angel.ml.GBDT.param.RegTParam;
import com.tencent.angel.ml.core.utils.Maths;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.utils.DataBlock;

import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Description: data information, always sit in memory.
 */
public class RegTDataStore {

  private static final Log LOG = LogFactory.getLog(RegTDataStore.class);


  public RegTParam param;

  public FeatureMeta featureMeta; // feature meta info

  public int numRow; // number of rows in the data
  public int numCol; // number of cols in the data
  public int numNonzero; // number of nonzero entries in the data
  public final static int kVersion = 1; // version flag, used to check version of this info

  public IntFloatVector[] instances; // training instances
  public float[] labels; // label of each instances
  public float[] preds; // pred of each instance
  //public int[] rootIndex; // specified root index of each instances, can be used for multi task setting
  //public int[] groupPtr; // the index of begin and end of a group, needed when the learning task is ranking.
  public float[] weights; // weights of each instances, optional

  public float[] baseWeights; // initial prediction to boost from.

  public RegTDataStore(RegTParam param) {
    this.param = param;
  }

  public void init(DataBlock<LabeledData> dataSet) throws IOException {
    numRow = dataSet.size();
    numCol = param.numFeature;
    numNonzero = param.numNonzero;
    instances = new IntFloatVector[numRow];
    labels = new float[numRow];
    preds = new float[numRow];
    weights = new float[numRow];
    baseWeights = new float[numRow];

    // max and min of each feature
    double[] minFeatures = new double[numCol];
    double[] maxFeatures = new double[numCol];
    Arrays.setAll(minFeatures, i -> 0.0f);
    Arrays.setAll(maxFeatures, i -> Float.MAX_VALUE);

    dataSet.resetReadIndex();
    LabeledData data;
    IntFloatVector x = null;
    double y;

    for (int idx = 0; idx < dataSet.size(); idx++) {
      data = dataSet.read();

      if (data.getX() instanceof IntFloatVector) {
        x = (IntFloatVector) data.getX();
      } else if (data.getX() instanceof IntDoubleVector) {
        x = VFactory.sparseFloatVector((int) data.getX().dim(),
            ((IntDoubleVector) data.getX()).getStorage().getIndices(),
            Maths.double2Float(((IntDoubleVector) data.getX()).getStorage().getValues()));
      }

      y = data.getY();
      if (y != 1) {
        y = 0;
      }

      int[] indices = x.getStorage().getIndices();
      float[] values = x.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        int fid = indices[i];
        double fvalue = values[i];
        if (fvalue > maxFeatures[fid]) {
          maxFeatures[fid] = fvalue;
        }
        if (fvalue < minFeatures[fid]) {
          minFeatures[fid] = fvalue;
        }
      }
      instances[idx] = x;
      labels[idx] = (float) y;
      preds[idx] = 0.0f;
      weights[idx] = 1.0f;
      baseWeights[idx] = 1.0f;
    }

    featureMeta =
        new FeatureMeta(numCol, Maths.double2Float(minFeatures), Maths.double2Float(maxFeatures));

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
  public void Clear() {
  }

  public void setNumRow(int numRow) {
    this.numRow = numRow;
  }

  public void setNumCol(int numCol) {
    this.numCol = numCol;
  }

  public void setNumNonzero(int numNonzero) {
    this.numNonzero = numNonzero;
  }

  public void setInstances(IntFloatVector[] instances) {
    this.instances = instances;
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
