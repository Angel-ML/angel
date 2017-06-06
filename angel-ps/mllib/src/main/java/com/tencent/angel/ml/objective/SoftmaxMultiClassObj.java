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

import com.tencent.angel.ml.RegTree.DataMeta;
import com.tencent.angel.ml.RegTree.GradPair;
import com.tencent.angel.ml.param.RegTTrainParam;
import com.tencent.angel.ml.utils.MathUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: Softmax for multi-class classification output class index if outputProb = false
 * output probability distribution if outputProb = true
 */

public class SoftmaxMultiClassObj implements ObjFunc {

  private static final Log LOG = LogFactory.getLog(SoftmaxMultiClassObj.class);

  public RegTTrainParam param;
  public int numClass;
  private boolean outputProb;

  public SoftmaxMultiClassObj(RegTTrainParam param, boolean outputProb) {
    this.param = param;
    this.numClass = param.numClass;
    this.outputProb = outputProb;
  }

  public SoftmaxMultiClassObj(RegTTrainParam param) {
    this(param, true);
  }

  /**
   * get gradient over each of predictions, given existing information.
   * 
   * @param preds: prediction of current round
   * @param dataMeta information about labels, weights, groups in rank
   * @param iteration current iteration number. return:_gpair output of get gradient, saves gradient
   *        and second order gradient in
   */
  @Override
  public List<GradPair> getGradient(float[] preds, DataMeta dataMeta, int iteration) {
    assert preds.length == this.numClass * dataMeta.labels.length;
    List<GradPair> rec = new ArrayList<GradPair>();
    int ndata = preds.length / numClass;
    int labelError = -1;
    float[] tmp = new float[numClass];
    for (int insIdx = 0; insIdx < ndata; insIdx++) {
      for (int k = 0; k < numClass; k++) {
        tmp[k] = preds[insIdx * numClass + k];
      }
      MathUtils.softmax(tmp);
      int label = (int) dataMeta.labels[insIdx];
      if (label < 0 || label >= numClass) {
        labelError = label;
        label = 0;
      }
      float wt = dataMeta.getWeight(insIdx);
      for (int k = 0; k < numClass; ++k) {
        float p = tmp[k];
        float h = 2.0f * p * (1.0f - p) * wt;
        if (label == k) {
          GradPair pair = new GradPair((p - 1.0f) * wt, h);
          rec.add(pair);
        } else {
          GradPair pair = new GradPair(p * wt, h);
          rec.add(pair);
        }
      }
    }
    if (labelError >= 0 && labelError < numClass) {
      LOG.error(String.format("SoftmaxMultiClassObj: label must be in [0, num_class), "
          + "numClass = %d, but found %d in label", numClass, labelError));
    }
    return rec;
  }

  public List<Float> transform(List<Float> preds, boolean prob) {
    List<Float> rec = new ArrayList<Float>();
    int ndata = preds.size() / numClass;
    float[] tmp = new float[numClass];

    for (int insIdx = 0; insIdx < ndata; insIdx++) {
      for (int k = 0; k < numClass; k++) {
        tmp[k] = preds.get(insIdx * numClass + k);
      }
      if (!prob) {
        rec.add((float) MathUtils.findMaxIndex(tmp));
      } else {
        MathUtils.softmax(tmp);
        for (int k = 0; k < numClass; k++) {
          preds.set(insIdx * numClass + k, tmp[k]);
        }
      }
    }
    // if (!prob) {
    // preds.clear();
    // preds.addAll(rec);
    // }
    return rec;
  }

  @Override
  public String defaultEvalMetric() {
    return "merror";
  }

  /**
   * transform prediction values, this is only called when Prediction is called preds: prediction
   * values, saves to this vector as well
   *
   * @param preds
   */
  @Override
  public void predTransform(List<Float> preds) {
    this.transform(preds, this.outputProb);
  }

  /**
   * transform prediction values, this is only called when Eval is called usually it redirect to
   * predTransform preds: prediction values, saves to this vector as well
   *
   * @param preds
   */
  @Override
  public void evalTransform(List<Float> preds) {
    this.transform(preds, true);
  }

  /**
   * transform probability value back to margin this is used to transform user-set base_score back
   * to margin used by gradient boosting
   *
   * @param base_score
   */
  @Override
  public float prob2Margin(float base_score) {
    return 0;
  }
}
