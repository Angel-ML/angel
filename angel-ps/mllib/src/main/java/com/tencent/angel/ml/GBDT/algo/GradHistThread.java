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

import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.matrix.psf.update.enhance.CompressUpdateFunc;
import com.tencent.angel.ml.model.PSModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GradHistThread implements Runnable {

  private static final Log LOG = LogFactory.getLog(GradHistThread.class);

  private final PSModel model;
  private final int nid;
  private final DenseDoubleVector histogram;
  private int bytesPerItem;

  public GradHistThread(PSModel model, int nid, DenseDoubleVector histogram, int bytesPerItem) {
    this.model = model;
    this.nid = nid;
    this.histogram = histogram;
    this.bytesPerItem = bytesPerItem;
  }

  @Override public void run() {
    LOG.debug(String.format("Run active node[%d]", this.nid));
    if (bytesPerItem < 1 || bytesPerItem > 8) {
      LOG.info("Invalid compress configuration: " + bytesPerItem + ", it should be [1,8].");
      bytesPerItem = MLConf.DEFAULT_ANGEL_COMPRESS_BYTES();
    }
    try {
      if (bytesPerItem == 8) {
        this.model.increment(0, histogram);
      } else {
        CompressUpdateFunc func =
          new CompressUpdateFunc(this.model.getMatrixId(), 0, histogram, bytesPerItem * 8);
        this.model.update(func);
      }
    } catch (Exception e) {
      LOG.error(model.modelName() + " increment failed, ", e);
    }
    LOG.debug(String.format("Active node[%d] finish", this.nid));
  }
}
