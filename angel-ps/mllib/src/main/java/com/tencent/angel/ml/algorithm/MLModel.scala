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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.worker.storage.Storage
import com.tencent.angel.worker.task.TaskContext

class MLModel(hdfsPath: String, matrixName : String) {
  var model: TDoubleVector = null

  def saveToLocalFile(path: String) = {
    // TODO
  }

  def loadFromHDFS() = {
    //TODO
  }

  def toPMML(path: String) = {
    // TODO
  }

  def predict(ctx: TaskContext, dataSet: Storage[LabeledData]) = {
    //TODO
  }
}
