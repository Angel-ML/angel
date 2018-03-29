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

package com.tencent.angel.ml.matrixfactorization

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.matrixfactorization.MFModel._
import com.tencent.angel.ml.matrixfactorization.threads.{ItemVec, UserVec}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

object MFModel {
  val MF_ITEM_MODEL = "mf_item_model"
  val MF_LOSS_MODEL = "mf_loss_mat"
  val MF_METRIC = "aver.obj"
}

/**
  * Matrix Factorization Model, consists item model and user model.
  * Item model is a [#item, rank] matrix sotred on PS, each row represents an item.
  * User model a UserVecs storage, each UserVec represents a user by a rank-dim vector.
  * To save storage, we only cached items that appears in the local worker trainning data.
  *
  * @param _ctx : context of running task
  * @param conf : configuration of algorithm
  */
class MFModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {


  // Number of all items.
  private val _itemNum = conf.getInt(MLConf.ML_MF_ITEM_NUM, MLConf.DEFAULT_ML_MF_ITEM_NUM)
  if (_itemNum < 1)
    throw new AngelException("Item number should be set positive.")
  // Value of rank.
  private val _K = conf.getInt(MLConf.ML_MF_RANK, MLConf.DEFAULT_ML_MF_RANK)

  // Reference of Item model matrix sotred on PS.
  // Each row is a k-dim item feature vector, represents an item.
  private val _itemMat = PSModel(MF_ITEM_MODEL, _itemNum, _K)
    .setAverage(true)
    .setRowType(RowType.T_FLOAT_DENSE)
    .setOplogType("DENSE_FLOAT")
  addPSModel(_itemMat)

  setSavePath(conf)
  setLoadPath(conf)

  // Users that appeared in local trainning dataset.
  private var _users = new mutable.HashMap[Int, UserVec]

  // Items that appeared in local trainning dataset.
  private var _items = new mutable.HashMap[Int, ItemVec]

  // Item Ids of items appeared locally
  private var _usedItemIds = new Array[Byte](totalItemNum)

  // Matrix Client for item model matrix
  def itemMat = ctx.getMatrix(_itemMat.modelName)

  def rank = _K

  def totalItemNum = _itemNum

  def userNum = _users.size

  def itemNum = _items.size

  def users = {
    _users
  }

  def items = {
    _items
  }

  def setUsedItem(itemId: Int) = {
    _usedItemIds(itemId) = 1
  }

  def usedItemIDs: Array[Byte] = {
    _usedItemIds
  }

  /**
    * Buid user vectors with local trainning data, only build users that appears in local
    * trainning dataset
    *
    * @param users
    */
  def buildUsers(users: mutable.HashMap[Int, UserVec]) = {
    _users = users
  }

  /**
    * Build item vectors with local trainning data, onlu build items that appears in local
    * trainning dataset
    *
    * @param items
    */
  def buildItems(items: mutable.HashMap[Int, ItemVec]) = {
    _items = items
  }


  override
  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = ???
}

