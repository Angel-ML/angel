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


package com.tencent.angel.ml.core.network.layers.verge

import java.lang.{Long => JLong}
import java.util.concurrent.Future
import java.util.{Map => JMap}

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.variable.MatVariable.MatrixType
import com.tencent.angel.ml.core.network.variable.Variable
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import org.apache.commons.logging.LogFactory

class Embedding(name: String, outputDim: Int, val numFactors: Int, override val optimizer: Optimizer)(implicit graph: Graph)
  extends InputLayer(name, outputDim)(graph) with Trainable {
  val LOG = LogFactory.getLog(classOf[Embedding])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf
  val modelType: RowType = SharedConf.modelType
  val blockSize: Int = SharedConf.blockSize

  private val embedding = Variable.getMatrix(s"${name}_embedding", numFactors,
    SharedConf.indexRange, SharedConf.modelSize, OptUtils.getSlotNum(optimizer), modelType, MatrixType.Embedding, location)


  @transient var forward: Matrix = _
  @transient var backward: Matrix = _

  override def calBackward(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        backward = gatherGrad()
        status = STATUS.Backward
      case _ =>
        throw new AngelException("Status Error, Should call forward before when calling backward!")
    }

    val end = System.currentTimeMillis()
    //    println(s"Embedding($name) calBackward = ${end - start} ms")
    backward
  }

  override def pullParams(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    embedding.pullParams(epoch, graph.placeHolder.getIndices)
    val end = System.currentTimeMillis()
  }

  def mergeUpdate(map: JMap[JLong, Vector], key: Long, update: Vector, value: Double): Unit = {
    if (!map.containsKey(key)) {
      if (value == 1) map.put(key, update)
      else map.put(key, update.imul(value))
    } else {
      if (value == 1) map.get(key).iadd(update)
      else map.get(key).iadd(update.imul(value))
    }
  }

  override def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Backward =>
        embedding.pushGrads(graph.placeHolder.getFeats, backward)
        status = STATUS.Gradient
      case _ =>
    }
    val end = System.currentTimeMillis()
  }

  override def update(epoch: Int, batchSize: Int): Future[VoidResult] = {
    var result: Future[VoidResult] = null
    status match {
      case STATUS.Gradient =>
        result = embedding.update(optimizer, epoch, batchSize)
        status = STATUS.Update
      case _ => print("STATUS Error, please calculate Gradient first!")
    }
    result
  }

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        forward = embedding.snapshot()
        status = STATUS.Forward
      case _ =>
    }

    val end = System.currentTimeMillis()
    // println(s"Embedding($name) calOutput ${end - start} ms")
    forward
  }

  override def init(taskFlag: Int): Unit = {
    embedding.init(taskFlag, mean = 0.0, stddev = 0.000001)
  }

  override def toString: String = {
    s"Embedding name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def loadParams(loadContext: ModelLoadContext): Unit = {
    embedding.loadParams(loadContext)
  }

  override def saveParams(saveContext: ModelSaveContext): Unit = {
    embedding.saveParams(saveContext)
  }
}
