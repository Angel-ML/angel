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

package com.tencent.angel.ml.treemodels.gbdt.fp

import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.ml.treemodels.gbdt.GBDTModel
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

object FPGBDTModel {
  private val LOG: Log = LogFactory.getLog(classOf[FPGBDTModel])

  def apply(conf: Configuration, _ctx: TaskContext = null): FPGBDTModel = new FPGBDTModel(conf, _ctx)
}

class FPGBDTModel(conf: Configuration, _ctx: TaskContext = null) extends GBDTModel(conf, _ctx) {
  private val LOG: Log = FPGBDTModel.LOG

  // Matrix 9: feature rows
  // TODO: create at runtime (after max nnz of feature is known)
  private val maxFeatRowSize = 1 + 2 * numWorker + Math.ceil(numFeatNnz * 5 / 4).toInt
  private val featRowMat = PSModel(GBDTModel.FEAT_ROW_MAT, batchSize, maxFeatRowSize, batchSize / numPS, maxFeatRowSize)
    .setRowType(RowType.T_FLOAT_DENSE)
    .setOplogType("DENSE_FLOAT")
    .setNeedSave(false)
  addPSModel(GBDTModel.FEAT_ROW_MAT, featRowMat)

  // Matrix 10: labels of all instances
  // TODO: create at runtime (after number of instances is known)
  private val labelsMat = PSModel(GBDTModel.LABEL_MAT, 1, numInstance, 1, numInstance / numPS)
    .setRowType(RowType.T_FLOAT_DENSE)
    .setOplogType("DENSE_FLOAT")
    .setHogwild(true)
    .setNeedSave(false)
  addPSModel(GBDTModel.LABEL_MAT, labelsMat)

  // Matrix 12: local best split feature id
  private val localBestFeat = PSModel(GBDTModel.LOCAL_SPLIT_FEAT_MAT, numWorker, maxNodeNum, numWorker / numPS, maxNodeNum)
    .setRowType(RowType.T_INT_SPARSE)
    .setOplogType("SPARSE_INT")
    .setHogwild(true)
    .setNeedSave(false)
  addPSModel(GBDTModel.LOCAL_SPLIT_FEAT_MAT, localBestFeat)

  // Matrix 13: local best split feature value
  private val localBestValue = PSModel(GBDTModel.LOCAL_SPLIT_VALUE_MAT, numWorker, maxNodeNum, numWorker / numPS, maxNodeNum)
    .setRowType(RowType.T_FLOAT_SPARSE)
    .setOplogType("SPARSE_FLOAT")
    .setHogwild(true)
    .setNeedSave(false)
  addPSModel(GBDTModel.LOCAL_SPLIT_VALUE_MAT, localBestValue)

  // Matrix 14: local best split feature gain
  private val localBestGain = PSModel(GBDTModel.LOCAL_SPLIT_GAIN_MAT, numWorker, maxNodeNum, numWorker / numPS, maxNodeNum)
    .setRowType(RowType.T_FLOAT_SPARSE)
    .setOplogType("SPARSE_FLOAT")
    .setHogwild(true)
    .setNeedSave(false)
  addPSModel(GBDTModel.LOCAL_SPLIT_GAIN_MAT, localBestGain)

  // Matrix 15: split result
  // TODO: create at runtime (after number of instances is known)
  private val colNum = Math.ceil(numInstance / 32.0).toInt
  private val splitResult = PSModel(GBDTModel.SPLIT_RESULT_MAT, 1, colNum, 1, colNum)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
    .setNeedSave(false)
  addPSModel(GBDTModel.SPLIT_RESULT_MAT, splitResult)

  super.setSavePath(conf)
  super.setLoadPath(conf)

}
