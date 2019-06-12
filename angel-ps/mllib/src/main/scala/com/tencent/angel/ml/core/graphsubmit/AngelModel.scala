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
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.network.{Graph, PlaceHolder}
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.ml.core.variable.{AngelCILSImpl, CILSImpl, VariableManager, VariableProvider}
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

class AngelModel(conf: Configuration, _ctx: TaskContext) extends GraphModel {
  lazy val sharedConf: SharedConf = SharedConf.get()
  lazy val batchSize: Int = SharedConf.batchSize
  lazy val blockSize: Int = SharedConf.blockSize

  //override protected val placeHolder: PlaceHolder = new PlaceHolder(sharedConf)
  override protected implicit val variableManager: VariableManager = PSVariableManager.get(isSparseFormat)
  protected implicit val cilsImpl: CILSImpl = new AngelCILSImpl()
  override protected val variableProvider: VariableProvider = new PSVariableProvider(dataFormat, modelType)

  implicit lazy val graph: Graph = if (_ctx != null) {
    new Graph(variableProvider, sharedConf, _ctx.getTotalTaskNum)
  } else {
    val totalTaskNum: Int = sharedConf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
      AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER) * sharedConf.getInt(
      AngelConf.ANGEL_TASK_ACTUAL_NUM, default = 1)
    new Graph(variableProvider, sharedConf, totalTaskNum)
  }

  override def buildNetwork(): this.type = {
    JsonUtils.layerFromJson(sharedConf.getJson)

    this
  }


  /**
    * Predict use the PSModels and predict data
    *
    * @param storage predict data
    * @return predict result
    */
  // def predict(storage: DataBlock[LabeledData]): List[PredictResult]
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
  def apply(className: String, conf: Configuration): AngelModel = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[Configuration], classOf[TaskContext])
    cstr.newInstance(conf, null).asInstanceOf[AngelModel]
  }

  def apply(className: String, conf: Configuration, ctx: TaskContext): AngelModel = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[Configuration], classOf[TaskContext])
    cstr.newInstance(conf, ctx).asInstanceOf[AngelModel]
  }
}
