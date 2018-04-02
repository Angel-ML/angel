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
import java.util.concurrent.atomic.AtomicInteger
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.fs.Path

import scala.collection.mutable


class PWriteUserTask(ctx: TaskContext, users: mutable.HashMap[Int, UserVec], idx: AtomicInteger,
                     pid: Int) extends Callable[Boolean] {
  val userIds = users.keySet.toArray

  @Override
  def call(): Boolean = {
    val conf = ctx.getConf()

    val dir = conf.get(MLConf.ML_MODEL_OUTPUT_PATH) + "/userVects/"
    System.out.print("user vector saved to " + dir)
    val taskID = ctx.getTaskId
    val path = dir + taskID + "_" + pid

    val outpath = new Path(path)
    val fs = outpath.getFileSystem(ctx.getConf)
    if (fs.exists(outpath)) {
      fs.delete(outpath)
    }

    val outStrem = fs.create(outpath)

    while (idx.getAndAdd(1) < users.size) {
      val user = userIds(idx.get() - 1)
      outStrem.writeBytes(users(user).toString + "\n")
    }
    true
  }

}