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
package com.tencent.angel.ml.algorithm.GBDT;

import com.tencent.angel.ml.algorithm.RegTree.DistributedHistHelper;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.psagent.matrix.MatrixClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ActiveTNodeRunner implements Runnable {

  private static final Log LOG = LogFactory.getLog(ActiveTNodeRunner.class);

  private final GBDTController model;
  private final int nid; // tree node id

  private final MatrixClient gradHistClient;

  public ActiveTNodeRunner(GBDTController model, int nid, MatrixClient gradHistClient) {
    this.model = model;
    this.nid = nid;
    this.gradHistClient = gradHistClient;
  }

  @Override
  public void run() {
    LOG.info(String.format("Run active node[%d]", this.nid));
    // 1. name of this node's grad histogram on PS
    String histParaName = this.model.param.gradHistNamePrefix + nid;
    // 2. build the grad histogram of this node
    DistributedHistHelper histMaker = new DistributedHistHelper(this.model, this.nid);
    DenseDoubleVector histogram = histMaker.buildHistogram();
    // 3. push the histograms to PS
    try {
      gradHistClient.increment(0, histogram);
    } catch (Exception e) {
      LOG.error(histParaName + " increment failed, ", e);
    }
    // LOG.info(String.format("Histogram: size[%d] %s", histogram.getDimension(),
    // Arrays.toString(histogram.getValues())));
    // 4. reset thread stats to finished
    this.model.activeNodeStat[this.nid] = 2;
    LOG.info(String.format("Active node[%d] finish", this.nid));
  }

}
