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
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.matrixfactorization

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, DenseFloatVector}
import com.tencent.angel.ml.model.{AlgorithmModel, PSModel}
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.{MemoryStorage, Storage}
import com.tencent.angel.worker.task.TaskContext
import com.tencent.angel.ml.algorithm.matrixfactorization.MFModel._
import com.tencent.angel.ml.algorithm.matrixfactorization.utils.{ItemVec, UserVec}
import com.tencent.angel.protobuf.generated.MLProtos.RowType
import org.apache.hadoop.conf.Configuration

import scala.collection.{Map, mutable}

object MFModel {
  val MF_ITEM_MODEL = "mf_item_model"
  val MF_LOSS_MODEL = "mf_loss_mat"
}

/**
  * Matrix Factorization Model, consists item model and user model.
  * Item model is a [#item, rank] matrix sotred on PS, each row represents an item.
  * User model a UserVecs storage, each UserVec represents a user by a rank-dim vector.
  * To save storage, we only cached items that appears in the local worker trainning data.
  * @param ctx  : context of running task
  * @param conf : configuration of algorithm
  */
class MFModel(ctx: TaskContext, conf: Configuration) extends AlgorithmModel {

  def this(conf: Configuration) = {
    this(null, conf)
  }

  // Number of all items.
  private val _itemNum = conf.getInt(MLConf.ML_MF_ITEM_NUM, -1)
  // Value of rank.
  private val _K = conf.getInt(MLConf.ML_MF_RANK, -1)

  // Reference of Item model matrix sotred on PS.
  // Each row is a k-dim item feature vector, represents an item.
  private val _itemMat = new PSModel[DenseFloatVector](MF_ITEM_MODEL, _itemNum, _K)
  _itemMat.setRowType(RowType.T_FLOAT_DENSE)
  _itemMat.setOplogType("DENSE_FLOAT")
  addPSModel(_itemMat)

  // Reference of loss value matrix stored on PS.
  val epoch: Int = conf.getInt(MLConf.ML_EPOCH_NUM, 10)
  private val _lossMat = new PSModel[DenseDoubleVector](MF_LOSS_MODEL, 1, epoch)
  addPSModel(_lossMat)

  // Users that appeared in local trainning dataset.
  private var _users = new mutable.HashMap[Int, UserVec]

  // Items that appeared in local trainning dataset.
  private var _items = new mutable.HashMap[Int, ItemVec]

  // Item Ids of items appeared locally
  private var _usedItemIds = new Array[Byte](totalItemNum)

  // Matrix Client for item model matrix
  def itemMat = {
    ctx.getMatrix(_itemMat.getName)
  }

  // Matrix Client for loss matrix on PS servers
  def lossMat = {
    ctx.getMatrix(_lossMat.getName)
  }


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
    * Buid user vectors with local trainning data
    * @param users
    */
  def buildUsers(users: mutable.HashMap[Int, UserVec]) = {
    _users = users
  }

  /**
    * Build item vectors with local trainning data
    * @param items
    */
  def buildItems(items: mutable.HashMap[Int, ItemVec]) = {
    _items = items
  }


  override
  def predict(storage: Storage[LabeledData]): Storage[PredictResult] = ???

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)
    _itemMat.setSavePath(path)
    _lossMat.setSavePath(path)
  }

  override
  def setLoadPath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_LOAD_MODEL_PATH)
    _itemMat.setLoadPath(path)
    _lossMat.setLoadPath(path)
  }
}

