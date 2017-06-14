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

import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;

import java.util.Arrays;

public class AvgQSketch {

  private int size;
  private double indices[];

  public AvgQSketch(int size) {
    this.size = size;
    this.indices = new double[size];
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
    System.out.println("Indices: " + Arrays.toString(indices));
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
    return ret;
  }

  public static void main(String[] argv) {
    System.out.println("begin");
    double[] vecValues = {9, 3, 1, 8, 7, 6, 2, 4, 5, 0};
    DenseDoubleVector vec = new DenseDoubleVector(10, vecValues);
    AvgQSketch sk = new AvgQSketch(10);
    sk.create(vec);
    System.out.println(sk.indexOf(9));
  }

}
