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

package com.tencent.angel.ml.algorithm.optimizer.admm;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDummyVector;

public class PredictScore {

  public static void predict(ADMMState state, double[] model) {

    for (LabeledData instance : state.instances) {
      SparseDummyVector features = (SparseDummyVector) instance.getX();

      double z = 0.0;

      int[] indices = features.getIndices();
      for (int i = 0; i < features.getNonzero(); i++) {
        int index = state.local2Global[indices[i]];
        z += model[index];
      }

      double score = 1.0 / (1.0 + Math.exp(-z));
      instance.setScore(score);
    }
  }

  public static void predict(ADMMState state, SparseDoubleVector model) {

    for (LabeledData instance : state.instances) {
      SparseDummyVector features = (SparseDummyVector) instance.getX();

      double z = 0.0;

      int[] indices = features.getIndices();

      for (int i = 0; i < features.getNonzero(); i++) {
        int index = state.local2Global[indices[i]];
        z += model.get(index);
      }

      double score = 1.0 / (1.0 + Math.exp(-z));
      instance.setScore(score);
    }
  }
}
