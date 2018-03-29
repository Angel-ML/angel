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

package com.tencent.angel.ml.treemodels.gbdt.dp

import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.ml.treemodels.gbdt.GBDTModel
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

object DPGBDTModel {
  private val LOG: Log = LogFactory.getLog(classOf[DPGBDTModel])

  def apply(conf: Configuration, _ctx: TaskContext = null): DPGBDTModel = new DPGBDTModel(conf, _ctx)
}

class DPGBDTModel(conf: Configuration, _ctx: TaskContext = null) extends GBDTModel(conf, _ctx) {
  private val LOG: Log = DPGBDTModel.LOG

  protected var numFeatSample = (numFeature * featSampleRatio).toInt
  if (numFeatSample % numPS != 0) {
    // adjust numFeatSample w.r.t. numPS
    numFeatSample = (numFeatSample / numPS + 1) * numPS
    if (numFeatSample > numFeature) {
      numFeatSample = numFeature // numFeature is already adjusted
    }
    LOG.info(s"Adjust sample feature num to $numFeatSample")
  }


  // TODO: use bitset
  private val featSampleMat = PSModel(GBDTModel.FEAT_SAMPLE_MAT, numTree, numFeatSample, 1, numFeatSample / numPS)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
    .setNeedSave(false)
  addPSModel(GBDTModel.FEAT_SAMPLE_MAT, featSampleMat)

  private val histMats = new Array[PSModel](maxNodeNum)
  for (nid <- 0 until maxNodeNum) {
    var size = numFeatSample * numSplit * 2
    if (numClass != 2) {
      size *= numClass
    }
    val histMat = PSModel(GBDTModel.GRAD_HIST_MAT_PREFIX + nid, 1, size, 1, size / numPS)
      .setRowType(RowType.T_FLOAT_DENSE)
      .setOplogType("DENSE_FLOAT")
      .setNeedSave(false)
    addPSModel(GBDTModel.GRAD_HIST_MAT_PREFIX + nid, histMat)
    histMats(nid) = histMat
  }
}
