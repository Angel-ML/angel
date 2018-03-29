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

package com.tencent.angel.ml.treemodels.gbdt.histogram;

import com.tencent.angel.ml.math.vector.DenseFloatVector;

/**
 * Histogram for GBDT nodes
 */
public class Histogram {
  private int numFeature;
  private int numSplit;
  private int numClass;

  private DenseFloatVector[] histograms;

  public Histogram(int numFeature, int numSplit, int numClass) {
    this.numFeature = numFeature;
    this.numSplit = numSplit;
    this.numClass = numClass;
    histograms = new DenseFloatVector[numFeature];
  }

  public void alloc() {
    int numHist = numClass == 2 ? 1 : numClass;
    int sizePerFeat = numHist * numSplit * 2;
    for (int i = 0; i < histograms.length; i++) {
      histograms[i] = new DenseFloatVector(sizePerFeat);
    }
  }

  public DenseFloatVector getHistogram(int index) {
    return histograms[index];
  }

  public void set(int index, DenseFloatVector hist) {
    this.histograms[index] = hist;
  }

  public Histogram plusBy(Histogram other) {
    for (int i = 0; i < histograms.length; i++) {
      int size = histograms[i].getDimension();
      float[] myValues = histograms[i].getValues();
      float[] otherValues = other.histograms[i].getValues();
      for (int j = 0; j < size; j++) {
        myValues[j] += otherValues[j];
      }
    }
    return this;
  }

  public Histogram subtract(Histogram other) {
    Histogram res = new Histogram(numFeature, numSplit, numClass);
    for (int i = 0; i < histograms.length; i++) {
      int size = histograms[i].getDimension();
      float[] resValue = new float[size];
      float[] aValue = this.histograms[i].getValues();
      float[] bValue = other.histograms[i].getValues();
      for (int j = 0; j < size; j++) {
        resValue[j] = aValue[j] - bValue[j];
      }
      res.histograms[i] = new DenseFloatVector(size, resValue);
    }
    return res;
  }

  public DenseFloatVector flatten() {
    int size = histograms.length * histograms[0].getDimension();
    float[] values = new float[size];
    int offset = 0;
    for (DenseFloatVector hist : histograms) {
      System.arraycopy(hist.getValues(), 0, values, offset, hist.getDimension());
      offset += hist.getDimension();
    }
    return new DenseFloatVector(size, values);
  }
}
