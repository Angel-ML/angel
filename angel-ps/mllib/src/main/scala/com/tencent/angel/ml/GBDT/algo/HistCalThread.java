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
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.concurrent.Callable;

public class HistCalThread implements Callable<Boolean> {

  private static final Log LOG = LogFactory.getLog(HistCalThread.class);

  private GBDTController controller;
  private int nid;
  private int start;
  private int end;

  public HistCalThread(GBDTController controller, int nid, int start, int end) {
    this.controller = controller;
    this.nid = nid;
    this.start = start;
    this.end = end;
  }

  @Override public Boolean call() throws Exception {
    GradHistHelper histMaker = new GradHistHelper(this.controller, this.nid);
    IntDoubleVector localHist = histMaker.buildHistogram(start, end);
    LOG.debug(String.format("Batch histogram[%d]: %s", nid,
      Arrays.toString(localHist.get(new int[] {0, 1, 2, 3, 4, 5}))));
    synchronized (this) {
      this.controller.histCache[nid].iadd(localHist);
      LOG.debug(String.format("Calculated histogram[%d]: %s", nid,
        Arrays.toString(this.controller.histCache[nid].get(new int[] {0, 1, 2, 3, 4, 5}))));
    }
    return true;
  }
}
