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

package com.tencent.angel.ml.matrixfactorization.threads

import java.util.concurrent.Callable
import com.tencent.angel.ml.math.vector.DenseFloatVector
import com.tencent.angel.ml.matrixfactorization.MFModel
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

import scala.collection.Map

class PSgdTask(var users: Map[Int, UserVec], var items: Map[Int, ItemVec],
               var context: TaskContext,
               var eta: Double,
               var lambda: Double,
               var rank: Int) extends Callable[Boolean] {
  var LOG = LogFactory.getLog(classOf[PSgdTask])
  val totalTaskNum = context.getTotalTaskNum

  var taskQueue = new GetRowsResult

  def setTaskQueue(queue: GetRowsResult) {
    this.taskQueue = queue
  }

  @throws[Exception]
  def call: Boolean = {
    var row: DenseFloatVector = null
    var loop = true
    while (loop) {
      row = taskQueue.poll.asInstanceOf[DenseFloatVector]
      if (row == null && taskQueue.isFetchOver)
        loop = false

      if (row == null) {
        Thread.sleep(10)
      } else {
        val itemId = row.getRowId
        val itemVec = items(itemId)
        val update = Utils.sgdOneItemVec(itemVec, row, users, eta, lambda, rank)

        val matrixClient = context.getMatrix(MFModel.MF_ITEM_MODEL)

        matrixClient.increment(itemId, update)

      }

    }

    return true
  }
}
