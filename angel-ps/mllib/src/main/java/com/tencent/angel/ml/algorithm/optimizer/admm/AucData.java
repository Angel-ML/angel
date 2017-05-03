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

import java.util.Arrays;

import com.tencent.angel.ml.feature.LabeledData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.TaskContext;

public class AucData {

  private static final Log LOG = LogFactory.getLog(AucData.class);

  public static final int bucketNum = 10000;

  public static double calcAuc(ADMMState state, TaskContext context) throws Exception {

    long posCnt = 0;
    long negCnt = 0;
    double[] tpCnt = new double[bucketNum];
    double[] fpCnt = new double[bucketNum];

    DenseIntVector update = new DenseIntVector(bucketNum * 2);

    for (LabeledData instance : state.instances) {
      double score = instance.getScore();
      if (score >= 0 && score <= 1.0 && instance.getY() >= 0 && instance.getY1() >= 0) {
        int neg = (int) instance.getY();
        int pos = (int) instance.getY1();

        int bucket = (int) (score * bucketNum);
        if (bucket == bucketNum)
          bucket--;

        int idx1 = bucket * 2;
        int idx2 = bucket * 2 + 1;

        update.set(idx1, update.get(idx1) + neg);
        update.set(idx2, update.get(idx2) + pos);
      }
    }

    MatrixClient aucClient = context.getMatrix(ADMMSubmitter.AUC);
    update.setMatrixId(context.getPSAgent().getMatrixMetaManager().getMatrixId(ADMMSubmitter.AUC));
    update.setRowId(0);

    aucClient.increment(update);
    // context.inc(update);

    aucClient.clock().get();
    // context.clockAndWait();

    DenseIntVector aucVector = (DenseIntVector) aucClient.getRow(0);
    // DenseIntVector aucVector = (DenseIntVector) context.getRow(ADMMSubmitter.AUC, 0);

    for (int i = 0; i < bucketNum; i++) {
      int neg = aucVector.get(i * 2);
      int pos = aucVector.get(i * 2 + 1);

      if (neg < 0 || pos < 0) {
        LOG.error("Overflow for bucket=" + i + " with neg=" + neg + " and pos=" + pos);
      }

      posCnt += pos;
      negCnt += neg;

      fpCnt[i] += neg;
      tpCnt[i] += pos;
    }

    // calc
    double auc = 0.0;

    int index = bucketNum - 1;
    while (index > 0) {
      fpCnt[index - 1] += fpCnt[index];
      tpCnt[index - 1] += tpCnt[index];
      index--;
    }

    if (posCnt > 0 && negCnt > 0) {
      for (int i = 0; i < bucketNum; i++) {
        tpCnt[i] /= posCnt;
        fpCnt[i] /= negCnt;
      }

      Arrays.sort(tpCnt);
      Arrays.sort(fpCnt);

      for (int i = 0; i < bucketNum - 1; i++) {
        double a = (tpCnt[i] + tpCnt[i + 1]) / 2;
        double b = fpCnt[i + 1] - fpCnt[i];
        auc += a * b;
      }
    }
    return auc;
  }
}
