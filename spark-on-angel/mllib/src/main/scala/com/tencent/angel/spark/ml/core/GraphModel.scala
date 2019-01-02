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


package com.tencent.angel.spark.ml.core


import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.{AngelGraph, PlaceHolder, STATUS}
import com.tencent.angel.ml.core.optimizer.decayer._
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import com.tencent.angel.ml.core.utils.paramsutils.JsonUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import com.tencent.angel.spark.context.AngelPSContext
import org.json4s.JsonAST.JValue

class GraphModel extends Serializable {

  val conf = SharedConf.get()
  implicit val graph = new AngelGraph(new PlaceHolder())
  var jsonAst: JValue = conf.getJson
  val stepSize: Double = SharedConf.learningRate
  val scheduler: StepSizeScheduler = StepSizeScheduler(SharedConf.getStepSizeScheduler, stepSize)

  def ensureJsonAst(): Unit = {
    if (jsonAst == null) {
      JsonUtils.init()
      jsonAst = conf.getJson
    }
  }

  def network(): Unit = {
    ensureJsonAst()
    JsonUtils.fillGraph(jsonAst)
  }

  def init(taskNum: Int): Unit = {
    network()

    graph.taskNum = taskNum
    graph.createMatrices()
    graph.init()
    println(s"graph=\n$graph")
  }

  def forward(epoch:Int, data: Array[LabeledData]): Matrix = {
    graph.feedData(data)
    graph.pullParams(epoch)
    graph.predict()
  }

  def getLoss(): Double = {
    graph.getOutputLayer.calLoss()
  }

  def getLossFunc(): LossFunc = {
    graph.getOutputLayer.getLossFunc
  }

  def backward(): Unit = {
    graph.calBackward()
    graph.pushGradient()
  }

  def update(iteration: Int, batchSize: Int): (Double, Boolean) = {
    val lr = scheduler.next()
    graph.setLR(lr)
    graph.setState(_ => true, STATUS.Gradient)
    graph.update(iteration, batchSize)
    (lr, scheduler.isIntervalBoundary)
  }

  def save(path: String): Unit = {
    val context = new ModelSaveContext(path)
    graph.getTrainable.map(layer => layer.saveParams(context))
    AngelPSContext.save(context)
  }

  def load(path: String): Unit = {
    val context = new ModelLoadContext(path)
    graph.getTrainable.map(layer => layer.loadParams(context))
    AngelPSContext.load(context)
  }

}

object GraphModel {
  def apply(className: String): GraphModel = {
    val cls = Class.forName(className)
    val constructor = cls.getConstructor()
    constructor.newInstance().asInstanceOf[GraphModel]
  }
}