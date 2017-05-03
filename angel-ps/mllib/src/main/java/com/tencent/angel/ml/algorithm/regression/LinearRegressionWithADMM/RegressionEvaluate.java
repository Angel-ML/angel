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

package com.tencent.angel.ml.algorithm.regression.LinearRegressionWithADMM;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class RegressionEvaluate {

  private static final Log LOG = LogFactory.getLog(RegressionEvaluate.class);

  public static void accuracy(ADMMLinearRegressionState state, DenseDoubleVector z) {
    List<LabeledData> instances = state.instances;

    Int2IntOpenHashMap biass = new Int2IntOpenHashMap();


    for (LabeledData instance : instances) {
      // double score = z.dot(instances.getX());
      double score = 0;
      SparseDummyVector point = (SparseDummyVector) instance.getX();
      int[] indices = point.getIndices();
      for (int i = 0; i < indices.length; i++) {
        score += z.get(state.local2Global[indices[i]]);
      }
      int predict = (int) (score + 0.5);
      int bias = (int) Math.abs(predict - instance.getY());
      biass.addTo(bias, 1);
    }

    int total = instances.size();
    for (int i = 0; i < 10; i++) {
      LOG.info("bias=" + i + " count=" + biass.get(i) / (double) total);
    }
  }
}
