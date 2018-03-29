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

package com.tencent.angel.ml.sketchML;

import com.tencent.angel.ml.math.vector.TDoubleVector;

public class AvgQSketch {

  private int size;
  private double[] indices;
  private int[] counts;

  public AvgQSketch(int size) {
    this.size = size;
    this.indices = new double[size];
    this.counts = new int[size];
  }

  public void create(TDoubleVector vec) {
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    for (int indice = 0; indice < vec.getDimension(); indice++) {
      if (vec.get(indice) < min) {
        min = vec.get(indice);
      }
      if (vec.get(indice) > max) {
        max = vec.get(indice);
      }
    }
    double span = (max - min) / size;
    for (int i = 0; i < size - 1; i++) {
      indices[i] = min + (i + 1) * span;
    }
    indices[size - 1] = max;
    for (int indice = 0; indice < vec.getDimension(); indice++) {
      counts[indexOf(vec.get(indice)) - 1]++;
    }
    //System.out.println("Indices: " + Arrays.toString(indices));
  }

  public int indexOf(double query) {
    int ret = 0;
    for (; ret < size - 1; ) {
      if (this.indices[ret + 1] <= query) {
        ret++;
      } else {
        break;
      }
    }
    return ret + 1;
  }

  public double get(int index) {
    return indices[index - 1];
  }

  public double[] getValues() {
    return indices;
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

  public double min() {
    return indices[0];
  }

  public double max() {
    return indices[indices.length - 1];
  }

  public int getZeroIndex() {
    return indexOf(0.0) + 1;
  }

}
