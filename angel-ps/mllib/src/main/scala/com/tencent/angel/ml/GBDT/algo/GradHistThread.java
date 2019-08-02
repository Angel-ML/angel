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


package com.tencent.angel.ml.GBDT.algo;

import com.tencent.angel.ml.GBDT.algo.RegTree.GradHistHelper;
import com.tencent.angel.ml.psf.compress.QuantifyDoubleFunc;
import com.tencent.angel.ml.core.conf.AngelMLConf;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;

import com.tencent.angel.ml.model.PSModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class GradHistThread implements Runnable {

  private static final Log LOG = LogFactory.getLog(GradHistThread.class);

  private final GBDTController controller;
  private final int nid; // tree node id
  private final PSModel model;
  private int insStart;
  private int insEnd;   // inclusive

  public GradHistThread(GBDTController controller, int nid, PSModel model, int start, int end) {
    this.controller = controller;
    this.nid = nid;
    this.model = model;
    this.insStart = start;
    this.insEnd = end;
  }

  @Override public void run() {
    LOG.debug(String.format("Run active node[%d]", this.nid));
    // 1. name of this node's grad histogram on PS
    String histParaName = this.controller.param.gradHistNamePrefix + nid;
    // 2. build the grad histogram of this node
    GradHistHelper histMaker = new GradHistHelper(this.controller, this.nid);
    IntDoubleVector histogram = histMaker.buildHistogram(insStart, insEnd);
    int bytesPerItem = this.controller.taskContext.getConf().
      getInt(AngelMLConf.ANGEL_COMPRESS_BYTES(), AngelMLConf.DEFAULT_ANGEL_COMPRESS_BYTES());
    if (bytesPerItem < 1 || bytesPerItem > 8) {
      LOG.info("Invalid compress configuration: " + bytesPerItem + ", it should be [1,8].");
      bytesPerItem = AngelMLConf.DEFAULT_ANGEL_COMPRESS_BYTES();
    }
    // 3. push the histograms to PS
    try {
      if (bytesPerItem == 8) {
        this.model.increment(0, histogram);
      } else {
        QuantifyDoubleFunc func =
          new QuantifyDoubleFunc(this.model.getMatrixId(), 0, histogram, bytesPerItem * 8);
        this.model.update(func);
      }
    } catch (Exception e) {
      LOG.error(histParaName + " increment failed, ", e);
    }
    // 4. reset thread stats to finished
    this.controller.activeNodeStat[this.nid].decrementAndGet();
    LOG.debug(String.format("Active node[%d] finish", this.nid));
  }

}