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

package com.tencent.angel.ml.sketchML;

import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.yahoo.sketches.quantiles.DoublesSketch;

public class YahooSketch {

  private int size;
  private double[] splits;
  private int[] counts;
  private int totalCount = 0;

  public YahooSketch(int size) {
    this.size = size;
    this.splits = new double[size];
    this.counts = new int[size];
  }

  public void create(TDoubleVector vec) {
    double[] fracs = new double[size];
    double min = Double.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      fracs[i] = i / (double) size;
    }

    int maxDim = vec.getDimension();

    DoublesSketch sketches = DoublesSketch.builder().build();

    for (int dim = 0; dim < maxDim; dim++) {
      if (Math.abs(vec.get(dim)) > 10e-12) {
        sketches.update(vec.get(dim));
        min = Math.min(min, vec.get(dim));
      }
    }

    splits = sketches.getQuantiles(fracs);

    for (int dim = 0; dim < maxDim; dim++) {
      if (Math.abs(vec.get(dim)) > 10e-12) {
        counts[indexOf(vec.get(dim))]++;
        totalCount++;
      }
    }
  }

  public int indexOf(double query) {
    int ret = 0;
    for (; ret < size - 1; ) {
      if (this.splits[ret + 1] <= query) {
        ret++;
      } else {
        break;
      }
    }
    return ret;
  }

  public double getSplit(int index) {
    return splits[index];
  }

  public double[] getSplits() {
    return splits;
  }

  public int[] getCounts() {
    return counts;
  }

  public int maxCount() {
    int ret = 0;
    for (int count : counts) {
      ret = Math.max(ret, count);
    }
    return ret;
  }

  public int totalCount() {
    return totalCount;
  }

  public double minSplit() {
    return splits[0];
  }

  public double maxSplit() {
    return splits[splits.length - 1];
  }

  public int zeroIndex() {
    return indexOf(0.0);
  }
}
