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
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult
import org.apache.commons.logging.LogFactory

import scala.collection.Map

class PEvaluateTask(var users: Map[Int, UserVec], var items: Map[Int, ItemVec],
                    var eta: Double, var lambda: Double, var rank: Int) extends Callable[Double] {
  val LOG = LogFactory.getLog(classOf[PEvaluateTask])
  private var taskQueue = new GetRowsResult

  def setTaskQueue(queue: GetRowsResult) {
    taskQueue = queue
  }

  @throws[Exception]
  def call: Double = {
    var row: DenseFloatVector = null
    var totalLoss = 0.0

    //    while (row != null && !taskQueue.isFetchOver) {
    //      row = taskQueue.poll.asInstanceOf[DenseFloatVector]
    //
    //      if (row != null) {
    //        val itemId = row.getRowId
    //        val itemVec = items(itemId)
    //        totalLoss += Utils.lossOneItemVec(itemVec, row, users, eta, lambda, rank)
    //      } else {
    //        Thread.sleep(10)
    //      }
    //    }

    var loop = true
    while (loop) {
      row = taskQueue.poll().asInstanceOf[DenseFloatVector]

      if (row == null && taskQueue.isFetchOver)
        loop = false

      if (row == null) {
        Thread.sleep(10)
      }
      else {
        val itemId = row.getRowId
        val itemVec = items(itemId)
        totalLoss += Utils.lossOneItemVec(itemVec, row, users, eta, lambda, rank)
      }


    }
    return totalLoss
  }
}
