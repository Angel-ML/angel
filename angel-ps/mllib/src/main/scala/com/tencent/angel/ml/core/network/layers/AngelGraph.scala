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


package com.tencent.angel.ml.core.network.layers

import java.util

import com.tencent.angel.client.AngelClient
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.verge.SimpleLossLayer
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.core.utils.paramsutils.ParamKeys
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import org.apache.commons.logging.{Log, LogFactory}
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class TimeStats(
                 var forwardTime: Long = 0,
                 var backwardTime: Long = 0,
                 var calGradTime: Long = 0,
                 var pullParamsTime: Long = 0,
                 var pushParamsTime: Long = 0,
                 var updateTime: Long = 0) extends Serializable {
  val LOG: Log = LogFactory.getLog(classOf[TimeStats])

  def summary(): String = {
    val summaryString = s"\nSummary: \n\t" +
      s"forwardTime = $forwardTime, \n\tbackwardTime = $backwardTime, \n\t" +
      s"calGradTime = $calGradTime, \n\tpullParamsTime = $pullParamsTime, \n\t" +
      s"pushParamsTime = $pushParamsTime, \n\tupdateTime = $updateTime"

    LOG.info(summaryString)
    summaryString
  }
}

class AngelGraph(val placeHolder: PlaceHolder, val conf: SharedConf) extends Serializable {

  def this(placeHolder: PlaceHolder) = this(placeHolder, SharedConf.get())

  val LOG: Log = LogFactory.getLog(classOf[AngelGraph])
  @transient private implicit val formats: DefaultFormats.type = DefaultFormats

  private val inputLayers = new ListBuffer[InputLayer]()
  private var lossLayer: LossLayer = _
  private val trainableLayer = new ListBuffer[Trainable]()
  @transient private val matrixCtxs = new ArrayBuffer[MatrixContext]()
  val timeStats = new TimeStats()
  var taskNum: Int = _

  def addInput(layer: InputLayer): Unit = {
    inputLayers.append(layer)
  }

  def setOutput(layer: LossLayer): Unit = {
    lossLayer = layer
  }

  def getOutputLayer: LossLayer = {
    lossLayer
  }

  def addTrainable(layer: Trainable): Unit = {
    trainableLayer.append(layer)
  }

  def getTrainable: ListBuffer[Trainable] = {
    trainableLayer
  }

  def addMatrixCtx(mc: MatrixContext): Unit = {
    matrixCtxs.append(mc)
  }

  def getMatrixCtx(): Seq[MatrixContext] = matrixCtxs

  def getLossLayer: LossLayer = lossLayer

  private def deepFirstDown(layer: Layer)(predicate: Layer => Boolean, action: Layer => Unit): Unit = {
    if (predicate(layer)) {
      action(layer)
      layer.input.foreach { lowerLayer =>
        deepFirstDown(lowerLayer)(predicate, action)
      }
    }
  }

  def setState(predicate: Layer => Boolean, status: STATUS.STATUS): Unit = {
    deepFirstDown(lossLayer.asInstanceOf[Layer])(
      predicate, (layer: Layer) => layer.status = status
    )
  }

  def feedData(data: Array[LabeledData]): Unit = {
    deepFirstDown(lossLayer.asInstanceOf[Layer])(
      (lay: Layer) => lay.status != STATUS.Null,
      (lay: Layer) => lay.status = STATUS.Null
    )

    placeHolder.feedData(data)
  }

  def predict(): Matrix = {
    val start = System.currentTimeMillis()
    val res = lossLayer.predict()
    timeStats.forwardTime += (System.currentTimeMillis() - start)
    res
  }

  def calLoss(): Double = {
    lossLayer.calLoss()
  }

  def calBackward(): Unit = {
    val start = System.currentTimeMillis()
    inputLayers.foreach { layer => layer.calBackward() }
    timeStats.backwardTime += (System.currentTimeMillis() - start)
  }

  def pullParams(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    trainableLayer.foreach { layer => layer.pullParams(epoch: Int) }
    timeStats.pullParamsTime += (System.currentTimeMillis() - start)
  }

  def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    trainableLayer.foreach(layer => layer.pushGradient())
    timeStats.pushParamsTime += (System.currentTimeMillis() - start)
  }

  def update(epoch: Int, batchSize: Int): Unit = {
    val start = System.currentTimeMillis()
    val updateFuture = trainableLayer.map(layer => layer.update(epoch, batchSize))
    for (future <- updateFuture) future.get
    timeStats.updateTime += (System.currentTimeMillis() - start)
  }

  def init(taskId: Int = 0): Unit = {
    trainableLayer.foreach { layer => layer.init(taskId) }
  }

  /**
    * Create matrices contain in the model, this method is only used in Driver/Client
    *
    * @param client Angel client
    */
  def createMatrices(client: AngelClient): Unit = {
    val contexts = new util.ArrayList[MatrixContext](matrixCtxs.size)
    matrixCtxs.foreach(context => contexts.add(context))
    client.createMatrices(contexts)
  }

  /**
    * Load model from files, this method is only used in Driver/Client
    *
    * @param client Angel client
    */
  def loadModel(client: AngelClient, path: String): Unit = {
    val loadContext = new ModelLoadContext(path)
    trainableLayer.foreach { layer => layer.loadParams(loadContext) }
    client.load(loadContext)
  }

  /**
    * Create matrices contain in the model, this method is only used in Worker/Executor
    */
  def createMatrices(): Unit = {
    PSMatrixUtils.createPSMatrix(matrixCtxs)
  }

  /**
    * Save model to files, this method is only use in Driver/Client
    *
    * @param client Angel client
    */
  def saveModel(client: AngelClient, path: String): Unit = {
    val saveContext = new ModelSaveContext(path)
    trainableLayer.foreach { layer => layer.saveParams(saveContext) }
    client.save(saveContext)
  }

  def setLR(lr: Double): Unit = {
    trainableLayer.foreach { trainable =>
      trainable.optimizer.setLR(lr)
    }
  }

  override def toString: String = {
    val str = new StringBuilder
    deepFirstDown(lossLayer.asInstanceOf[Layer])(_ => true, layer => str.append(layer.toString + "\n"))
    str.toString()
  }

  def toJson: JArray = {
    val jarr = ArrayBuffer.empty[JValue]
    val nameSet = new util.HashSet[String]
    layer2Json(lossLayer.asInstanceOf[Layer], jarr, nameSet)

    JArray(mergeFCLayer(jarr.toList))
  }

  private def layer2Json(layer: Layer, jarr: ArrayBuffer[JValue], nameSet: util.HashSet[String]): Unit = {
    layer match {
      case loss: SimpleLossLayer if !nameSet.contains(loss.name) =>
        layer2Json(loss.inputLayer, jarr, nameSet)
        val obj: JObject = loss.toJson
        val name = (obj \ ParamKeys.name).extract[String]
        nameSet.add(name)
        jarr.append(obj)
      case join: JoinLayer => !nameSet.contains(join.name)
        join.inputLayers.foreach { l =>
          layer2Json(l, jarr, nameSet)
        }
        val obj: JObject = join.toJson
        val name = (obj \ ParamKeys.name).extract[String]
        nameSet.add(name)
        jarr.append(obj)
      case ip: InputLayer if !nameSet.contains(ip.name) =>
        val obj: JObject = ip.toJson
        val name = (obj \ ParamKeys.name).extract[String]
        nameSet.add(name)
        jarr.append(obj)
      case linear: LinearLayer if !nameSet.contains(linear.name) =>
        layer2Json(linear.inputLayer, jarr, nameSet)
        val obj: JObject = linear.toJson
        val name = (obj \ ParamKeys.name).extract[String]
        nameSet.add(name)
        jarr.append(obj)
      case _ =>
    }
  }

  private def mergeFCLayer(jarr: List[JValue]): List[JValue] = {
    val abuf = ArrayBuffer.empty[JValue]
    val fcLayer = ArrayBuffer.empty[JValue]

    jarr.foreach {
      case jobj: JObject =>
        if (!(jobj \ ParamKeys.typeName).extract[String].equalsIgnoreCase("FCLayer")) {
          if (fcLayer.nonEmpty) {
            abuf.append(doMerge(fcLayer))
            fcLayer.clear()
          }
          abuf.append(jobj)
        } else {
          fcLayer.append(jobj)
        }
    }

    abuf.toList
  }

  private def doMerge(abuf: ArrayBuffer[JValue]): JValue = {
    val names = mutable.HashSet[String]()
    val inputlayers = mutable.HashSet[String]()

    abuf.foreach {
      case jobj: JObject =>
        names.add((jobj \ ParamKeys.name).extract[String])
        inputlayers.add((jobj \ ParamKeys.inputLayer).extract[String])
      case _ =>
    }

    val input = inputlayers.toSet.diff(names).head
    val output = names.toSet.diff(inputlayers).head

    var currInput = input
    val outputdims = mutable.ListBuffer[JValue]()
    val transfuncs = mutable.ListBuffer[JValue]()

    abuf.indices.foreach { _ =>
      abuf.collectFirst { case jobj: JObject if (jobj \ ParamKeys.inputLayer).extract[String] == currInput =>
        val name_ = (jobj \ ParamKeys.name).extract[String]
        val input_ = (jobj \ ParamKeys.inputLayer).extract[String]
        assert(input_ == currInput)
        val outputDim = jobj \ ParamKeys.outputDim
        outputdims.append(outputDim)

        val transFunc = jobj \ ParamKeys.transFunc
        transfuncs.append(transFunc)
        currInput = name_
      }
    }

    (ParamKeys.name -> output) ~ (ParamKeys.typeName -> "FCLayer") ~
      (ParamKeys.outputDims -> JArray(outputdims.toList)) ~
      (ParamKeys.inputLayer, JString(input)) ~
      (ParamKeys.transFuncs -> JArray(transfuncs.toList)) ~
      (ParamKeys.optimizer -> (abuf.head \ ParamKeys.optimizer))

  }
}
