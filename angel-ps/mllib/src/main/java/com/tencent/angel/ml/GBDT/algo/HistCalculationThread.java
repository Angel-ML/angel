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
package com.tencent.angel.ml.GBDT.algo;

import com.tencent.angel.ml.GBDT.algo.RegTree.GradHistHelper;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.model.PSModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Callable;

public class HistCalculationThread implements Callable<DenseDoubleVector> {

  private static final Log LOG = LogFactory.getLog(HistCalculationThread.class);

  private final GBDTController controller;
  private final int nid;
  private final int start;
  private final int end;
  private final int bytesPerItem;

  public HistCalculationThread(GBDTController controller, int nid, int start, int end, int bytesPerItem) {
    this.controller = controller;
    this.nid = nid;
    this.start = start;
    this.end = end;
    this.bytesPerItem = bytesPerItem;
  }

  @Override public DenseDoubleVector call() {
    LOG.debug(String.format("Calculate active node[%d]", this.nid));

    String histParaName = controller.param.gradHistNamePrefix + nid;
    PSModel model = controller.model.getPSModel(histParaName);

    GradHistHelper histMaker = new GradHistHelper(controller, nid);
    DenseDoubleVector histogram = histMaker.buildHistogram(start, end);
    histMaker.pushHistogram(model, histogram, bytesPerItem);
    LOG.debug(String.format("Active node[%d] finish", this.nid));
    return histogram;
  }
}
