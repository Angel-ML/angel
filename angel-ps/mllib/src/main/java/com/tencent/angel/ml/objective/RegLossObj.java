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
package com.tencent.angel.ml.objective;

import com.tencent.angel.ml.RegTree.RegTDataStore;
import com.tencent.angel.ml.RegTree.GradPair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 */

public class RegLossObj implements ObjFunc {

  private static final Log LOG = LogFactory.getLog(RegLossObj.class);

  private LossHelper loss;
  private float scalePosWeight;

  public RegLossObj(LossHelper loss) {
    this.loss = loss;
    this.scalePosWeight = 1.0f;
  }

  /**
   * get gradient over each of predictions, given existing information. preds: prediction of current
   * round info information about labels, weights, groups in rank iteration current iteration
   * number. return:_gpair output of get gradient, saves gradient and second order gradient in
   *
   * @param preds: predictive value
   * @param dataStore: data meta info
   * @param iteration: current interation
   */
  @Override
  public List<GradPair> calGrad(float[] preds, RegTDataStore dataStore, int iteration) {
    assert preds.length > 0;
    assert preds.length == dataStore.labels.length;
    List<GradPair> rec = new ArrayList<GradPair>();
    int ndata = preds.length; // number of data instances
    // check if label in range
    boolean label_correct = true;
    for (int i = 0; i < ndata; i++) {
      float p = loss.transPred(preds[i]);
      float w = dataStore.getWeight(i);
      if (dataStore.labels[i] == 1.0f)
        w *= scalePosWeight;
      if (!loss.checkLabel(dataStore.labels[i]))
        label_correct = false;
      GradPair pair =
          new GradPair(loss.firOrderGrad(p, dataStore.labels[i]) * w, loss.secOrderGrad(p,
                  dataStore.labels[i]) * w);
      rec.add(pair);
    }
    if (!label_correct) {
      LOG.error(loss.labelErrorMsg());
    }
    return rec;
  }

  /**
   * transform prediction values, this is only called when Prediction is called preds: prediction
   * values, saves to this vector as well
   *
   * @param preds
   */
  @Override
  public void transPred(List<Float> preds) {
    int ndata = preds.size();
    for (int j = 0; j < ndata; ++j) {
      preds.set(j, loss.transPred(preds.get(j)));
    }
  }

  /**
   * transform prediction values, this is only called when Eval is called usually it redirect to
   * transPred preds: prediction values, saves to this vector as well
   *
   * @param preds
   */
  @Override
  public void transEval(List<Float> preds) {
    this.transPred(preds);
  }

  /**
   * transform probability value back to margin this is used to transform user-set base_score back
   * to margin used by gradient boosting
   *
   * @param base_score
   */
  @Override
  public float prob2Margin(float base_score) {
    return loss.prob2Margin(base_score);
  }

  @Override
  public String defaultEvalMetric() {
    return loss.defaultEvalMetric();
  }
}
