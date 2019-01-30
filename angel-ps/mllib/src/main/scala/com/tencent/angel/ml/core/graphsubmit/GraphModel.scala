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

import com.tencent.angel.ml.core.AngelGraph
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.optimizer.loss._
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

import scala.reflect.ClassTag


class GraphModel(conf: Configuration, _ctx: TaskContext = null)
  extends MLModel(conf, _ctx) {
  val sharedConf: SharedConf = SharedConf.get()
  val batchSize: Int = SharedConf.batchSize
  val blockSize: Int = SharedConf.blockSize
  val dataFormat: String = SharedConf.inputDataFormat

  implicit lazy val graph: Graph = new AngelGraph(new PlaceHolder(sharedConf), sharedConf,
    _ctx.getTotalTaskNum)

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
  override def predict[T: ClassTag](storage: DataBlock[LabeledData]): DataBlock[T] = {
    val resData = new MemoryDataBlock[T](storage.size())

    var count: Int = 0
    while (count < storage.size()) {
      val numSamples = if (count + batchSize <= storage.size()) {
        batchSize
      } else {
        storage.size() - count
      }
      val batchData = new Array[LabeledData](numSamples)

      (0 until numSamples).foreach {
        idx => batchData(idx) = storage.loopingRead()
      }

      graph.predict() foreach { res => resData.put(res.asInstanceOf[T]) }

      count += batchSize
    }

    resData
  }

  def init(taskFlag: Int): Unit = {
    graph.init(taskFlag)
  }

  def createMatrices(envCtx: EvnContext): Unit = {
    graph.createMatrices(envCtx)
  }

  def loadModel(envCtx: EvnContext, path: String): Unit = {
    graph.loadModel(envCtx, path)
  }

  def saveModel(envCtx: EvnContext, path: String): Unit = {
    graph.saveModel(envCtx, path)
  }
}

object GraphModel {
  def apply(className: String, conf: Configuration): GraphModel = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[Configuration], classOf[TaskContext])
    cstr.newInstance(conf, null).asInstanceOf[GraphModel]
  }

  def apply(className: String, conf: Configuration, ctx: TaskContext = null): GraphModel = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[Configuration], classOf[TaskContext])
    cstr.newInstance(conf, ctx).asInstanceOf[GraphModel]
  }
}
