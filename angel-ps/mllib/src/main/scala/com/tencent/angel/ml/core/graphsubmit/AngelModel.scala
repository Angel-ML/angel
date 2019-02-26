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
import com.tencent.angel.ml.core.{AngelGraph, GraphModel, PredictResult}
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.optimizer.loss._
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

import scala.reflect.runtime.{universe => ru}

class AngelModel(conf: Configuration, _ctx: TaskContext) extends GraphModel {
  lazy val sharedConf: SharedConf = SharedConf.get()
  lazy val batchSize: Int = SharedConf.batchSize
  lazy val blockSize: Int = SharedConf.blockSize
  lazy val dataFormat: String = SharedConf.inputDataFormat

  implicit lazy val graph: Graph = if (_ctx!= null) {
    new AngelGraph(new PlaceHolder(sharedConf), sharedConf, _ctx.getTotalTaskNum)
  } else {
    val totalTaskNum: Int = sharedConf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
      AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER) * sharedConf.getInt(
      AngelConf.ANGEL_TASK_ACTUAL_NUM, default = 1)
    new AngelGraph(new PlaceHolder(sharedConf), sharedConf, totalTaskNum)
  }

  def lossFunc: LossFunc = graph.getLossFunc

  def buildNetwork(): Unit = {
    JsonUtils.layerFromJson(sharedConf.getJson)
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
    (0 until numSamples).foreach{idx => batchData(idx) = storage.loopingRead()}
    graph.feedData(batchData)

    if (graph.asInstanceOf[AngelGraph].isSparseFormat) {
      pullParams(-1, graph.placeHolder.getIndices)
    } else {
      pullParams(-1)
    }

    graph.predict()
  }
}

object AngelModel {
  def apply(className: String, conf: Configuration): AngelModel = {
//    val rtMirror = ru.runtimeMirror(getClass.getClassLoader)
//
//    val clsType = ru.typeOf[AngelModel]
//    val angelModel  = clsType.typeSymbol.asClass
//    val clsMirror = rtMirror.reflectClass(angelModel)
//
//    val ctor = clsType.decl(ru.termNames.CONSTRUCTOR).asMethod
//    val ctorm = clsMirror.reflectConstructor(ctor)
//
//    ctorm(conf, null).asInstanceOf[AngelModel]
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
