/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.core.graphsubmit

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core._
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.network.Graph
import com.tencent.angel.mlcore.utils.JsonUtils
import com.tencent.angel.mlcore.variable.{VariableManager, VariableProvider}
import com.tencent.angel.ml.core.variable.{AngelCILSImpl, CILSImpl}
import com.tencent.angel.ml.math2.utils.{DataBlock, LabeledData}
import com.tencent.angel.mlcore.{GraphModel, PredictResult}
import com.tencent.angel.worker.task.TaskContext


class AngelModel(conf: SharedConf, ctx: TaskContext = null) extends GraphModel(conf) {
  lazy val batchSize: Int = conf.batchSize
  lazy val blockSize: Int = conf.blockSize
  private implicit val sharedConf: SharedConf = conf

  override protected implicit val variableManager: VariableManager = new PSVariableManager(isSparseFormat, conf)
  protected implicit val cilsImpl: CILSImpl = new AngelCILSImpl(conf)
  override protected val variableProvider: VariableProvider = new PSVariableProvider(dataFormat, sharedConf)
  var taskNum: Int = -1
  if (ctx != null) taskNum = ctx.getTotalTaskNum

  implicit lazy val graph: Graph = if (taskNum != -1) {
    new Graph(variableProvider, conf, taskNum)
  } else {
    val _taskNum: Int = conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
      AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER) * conf.getInt(
      AngelConf.ANGEL_TASK_ACTUAL_NUM, default = 1)
    new Graph(variableProvider, conf, _taskNum)
  }

  override def buildNetwork(): this.type = {
    JsonUtils.layerFromJson(graph)

    this
  }


  /**
    * Predict use the PSModels and predict data
    *
    * @param storage predict data
    * @return predict result
    */
  override def predict(storage: DataBlock[LabeledData]): List[PredictResult] = {
    // new MemoryDataBlock[PredictResult](storage.size())
    val numSamples = storage.size()
    val batchData = new Array[LabeledData](numSamples)
    (0 until numSamples).foreach { idx => batchData(idx) = storage.loopingRead() }
    graph.feedData(batchData)

    val indices = graph.placeHolder.getIndices

    if (isSparseFormat) {
      pullParams(-1, indices)
    } else {
      pullParams(-1)
    }

    graph.predict()
  }

  override def predict(storage: LabeledData): PredictResult = ???
}

object AngelModel {
  def apply(className: String, conf: SharedConf): AngelModel = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[SharedConf], classOf[TaskContext])
    cstr.newInstance(conf, null).asInstanceOf[AngelModel]
  }

  def apply(className: String, conf: SharedConf, ctx: TaskContext = null): AngelModel = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[SharedConf], classOf[TaskContext])
    cstr.newInstance(conf, ctx).asInstanceOf[AngelModel]
  }
}