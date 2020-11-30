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


import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers.{AngelGraph, PlaceHolder, STATUS}
import com.tencent.angel.ml.core.optimizer.decayer._
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import com.tencent.angel.ml.core.utils.paramsutils.{JsonUtils, ParamKeys}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import com.tencent.angel.spark.context.AngelPSContext
import com.tencent.angel.utils.HdfsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}

class GraphModel extends Serializable {

  val sharedConf: SharedConf = SharedConf.get()
  implicit val graph: AngelGraph = new AngelGraph(new PlaceHolder())
  var jsonAst: JValue = sharedConf.getJson
  val stepSize: Double = SharedConf.learningRate
  val scheduler: StepSizeScheduler = StepSizeScheduler(SharedConf.getStepSizeScheduler, stepSize)

  def ensureJsonAst(): Unit = {
    if (jsonAst == null) {
      JsonUtils.init()
      jsonAst = sharedConf.getJson
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

  def forward(epoch: Int, data: Array[LabeledData]): Matrix = {
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
    graph.getTrainable.foreach(layer => layer.saveParams(context))
    AngelPSContext.save(context)
  }

  def load(path: String): Unit = {
    val context = new ModelLoadContext(path)
    graph.getTrainable.foreach(layer => layer.loadParams(context))
    AngelPSContext.load(context)
  }

  def saveJson(path: String, conf: Configuration): Unit = {
    val jsonFile: Path = new Path(path, "graph.json")
    val tmpJsonFile = HdfsUtil.toTmpPath(jsonFile)
    val fs: FileSystem = tmpJsonFile.getFileSystem(conf)
    val jsonOut: FSDataOutputStream = fs.create(tmpJsonFile)
    jsonOut.writeBytes(pretty(render(toJson)))
    jsonOut.flush()
    jsonOut.close()
    HdfsUtil.rename(tmpJsonFile, jsonFile, fs)
  }

  def toJson: JObject = {
    val data = (ParamKeys.format -> JString(SharedConf.inputDataFormat)) ~
      // (ParamKeys.indexRange -> JLong(SharedConf.indexRange)) ~
      (ParamKeys.indexRange -> JInt(SharedConf.indexRange)) ~
      (ParamKeys.numField -> JInt(sharedConf.getInt(MLConf.ML_FIELD_NUM))) ~
      (ParamKeys.validateRatio -> JDouble(sharedConf.getDouble(MLConf.ML_VALIDATE_RATIO))) ~
      (ParamKeys.sampleRatio -> JDouble(sharedConf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO)))

    val model = (ParamKeys.modelType -> JString(SharedConf.modelType.toString)) ~
      // (ParamKeys.modelSize -> JLong(SharedConf.modelSize)) ~
      (ParamKeys.modelSize -> JInt(SharedConf.modelSize)) ~
      (ParamKeys.blockSize -> JInt(sharedConf.getInt(MLConf.ML_BLOCK_SIZE)))

    val train = (ParamKeys.epoch -> JInt(SharedConf.epochNum)) ~
      (ParamKeys.numUpdatePerEpoch -> JInt(SharedConf.numUpdatePerEpoch)) ~
      (ParamKeys.batchSize -> JInt(SharedConf.batchSize)) ~
      (ParamKeys.lr -> JDouble(sharedConf.getDouble(MLConf.ML_LEARN_RATE))) ~
      (ParamKeys.decayClass -> JString(sharedConf.getString(MLConf.ML_OPT_DECAY_CLASS_NAME))) ~
      (ParamKeys.decayAlpha -> JDouble(sharedConf.getDouble(MLConf.ML_OPT_DECAY_ALPHA))) ~
      (ParamKeys.decayBeta -> JDouble(sharedConf.getDouble(MLConf.ML_OPT_DECAY_BETA)))

    (ParamKeys.data -> data) ~
      (ParamKeys.model -> model) ~
      (ParamKeys.train -> train) ~
      (ParamKeys.layers -> graph.toJson)
  }
}

object GraphModel {
  def apply(className: String): GraphModel = {
    val cls = Class.forName(className)
    val constructor = cls.getConstructor()
    constructor.newInstance().asInstanceOf[GraphModel]
  }
}