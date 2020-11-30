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


package com.tencent.angel.ml.core.utils.paramsutils

import java.io.File

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.linear.FCLayer
import com.tencent.angel.ml.core.network.layers.{AngelGraph, Layer}
import com.tencent.angel.ml.core.optimizer.Optimizer
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.io.Source

object JsonUtils {

  def init(): Unit = {
    val sharedConf = SharedConf.get()
    val jsonFileName = sharedConf.get(AngelConf.ANGEL_ML_CONF)

    if (jsonFileName == null || jsonFileName.length == 0) {
      throw new AngelException("angel.ml.conf not set!")
    } else {
      val file = new File(jsonFileName)
      if (file.exists()) {
        val json = parseJson(jsonFileName)
        updateConf(json, sharedConf)
        sharedConf.setJson(json)
      } else {
        val nameWithoutPath = file.getName
        if (new File(nameWithoutPath).exists()) {
          val json = parseJson(nameWithoutPath)
          updateConf(json, sharedConf)
          sharedConf.setJson(json)
        } else {
          throw new AngelException("Cannot find json conf file !")
        }
      }
    }
  }

  private def updateConf(json: JValue, conf: SharedConf): Unit = {
    json match {
      case JNothing =>
      case jast: JValue =>
        jast \ ParamKeys.data match {
          case JNothing =>
          case js: JValue => DataParams(js).updateConf(conf)
        }

        jast \ ParamKeys.train match {
          case JNothing =>
          case js: JValue => TrainParams(js).updateConf(conf)
        }

        jast \ ParamKeys.model match {
          case JNothing =>
          case js: JValue => ModelParams(js).updateConf(conf)
        }
    }
  }

  def fillGraph(jsonAst: JValue)(implicit graph: AngelGraph): Unit = {
    implicit val network: mutable.HashMap[String, Layer] = new mutable.HashMap[String, Layer]()
    import LayerParams._

    val defaultOpt = jsonAst \ ParamKeys.defaultOptimizer match {
      case JNothing => None
      case jast: JValue => Some(OptParams(jast))
    }

    val defaultTrans = jsonAst \ ParamKeys.defaultTransFunc match {
      case JNothing => None
      case jast: JValue => Some(TransFuncParams(jast))
    }

    val defaultLoss = jsonAst \ ParamKeys.defaultLossFunc match {
      case JNothing => None
      case jast: JValue => Some(LossFuncParams(jast))
    }

    var lossLayer: LayerParams = null
    val layerList = jsonAst \ ParamKeys.layers match {
      case layerList: JArray => layerList.arr.map { layer =>
        val layerParams = LayerParams(layer, defaultLoss, defaultTrans, defaultOpt)
        layerParams.layerType.trim.toLowerCase match {
          case `simpleLoss` | "LossLayer" =>
            lossLayer = layerParams
          case _ =>
        }
        layerParams.name -> layerParams
      }
      case _ => throw new AngelException("there is no layer conf")
    }

    assert(lossLayer != null)
    lossLayer.build(layerList.toMap)
  }

  def getOptimizerByLayerType(jsonAst: JValue, layerType: String): Optimizer = {
    val defaultOpt = jsonAst \ ParamKeys.defaultOptimizer match {
      case jast: JValue => Some(OptParams(jast))
    }

    var res: Optimizer = null
    jsonAst \ ParamKeys.layers match {
      case layerList: JArray => layerList.arr.foreach { layer =>
        val layerParams = LayerParams(layer, None, None, defaultOpt)
        layerParams match {
          case p: InputLayerParams if p.layerType.equalsIgnoreCase(layerType) =>
            res = p.optimizer.build()
          case p: EmbeddingParams if p.layerType.equalsIgnoreCase(layerType) =>
            res = p.optimizer.build()
          case p: FCLayerParams if p.layerType.equalsIgnoreCase(layerType) =>
            res = p.optimizer.build()
          case _ =>
        }
      }
    }
    if (res == null && defaultOpt.isDefined) {
      res = defaultOpt.get.build()
    }

    assert(res != null)
    res
  }

  def getOptimizerByLayerName(jsonAst: JValue, name: String): Optimizer = {
    val defaultOpt = jsonAst \ ParamKeys.defaultOptimizer match {
      case jast: JValue => Some(OptParams(jast))
    }

    var res: Optimizer = null
    jsonAst \ ParamKeys.layers match {
      case layerList: JArray => layerList.arr.foreach { layer =>
        val layerParams = LayerParams(layer, None, None, defaultOpt)
        layerParams match {
          case p: InputLayerParams if p.name.equalsIgnoreCase(name) =>
            res = p.optimizer.build()
          case p: EmbeddingParams if p.name.equalsIgnoreCase(name) =>
            res = p.optimizer.build()
          case p: FCLayerParams if p.name.equalsIgnoreCase(name) =>
            res = p.optimizer.build()
          case _ => throw new AngelException("")
        }
      }
    }
    if (res == null && defaultOpt.isDefined) {
      res = defaultOpt.get.build()
    }

    assert(res != null)
    res
  }

  def getLayerParamsByLayerType(jsonAst: JValue, layerType: String): LayerParams = {
    val defaultOpt = jsonAst \ ParamKeys.defaultOptimizer match {
      case JNothing => None
      case jast: JValue => Some(OptParams(jast))
    }

    val defaultTrans = jsonAst \ ParamKeys.defaultTransFunc match {
      case JNothing => None
      case jast: JValue => Some(TransFuncParams(jast))
    }

    val defaultLoss = jsonAst \ ParamKeys.defaultLossFunc match {
      case JNothing => None
      case jast: JValue => Some(LossFuncParams(jast))
    }

    var layerParams: LayerParams = null
    jsonAst \ ParamKeys.layers match {
      case JNothing =>
      case layerList: JArray => layerList.arr.foreach { layer =>
        LayerParams(layer, defaultLoss, defaultTrans, defaultOpt) match {
          case p if p.layerType.equalsIgnoreCase(layerType) =>
            layerParams = p
          case _ =>
        }
      }
    }

    assert(layerParams != null)
    layerParams
  }

  def getLayerParamsByLayerName(jsonAst: JValue, name: String): LayerParams = {
    val defaultOpt = jsonAst \ ParamKeys.defaultOptimizer match {
      case JNothing => None
      case jast: JValue => Some(OptParams(jast))
    }

    val defaultTrans = jsonAst \ ParamKeys.defaultTransFunc match {
      case JNothing => None
      case jast: JValue => Some(TransFuncParams(jast))
    }

    val defaultLoss = jsonAst \ ParamKeys.defaultLossFunc match {
      case JNothing => None
      case jast: JValue => Some(LossFuncParams(jast))
    }

    var layerParams: LayerParams = null
    jsonAst \ ParamKeys.layers match {
      case layerList: JArray => layerList.arr.foreach { layer =>
        LayerParams(layer, defaultLoss, defaultTrans, defaultOpt) match {
          case p if p.name.equalsIgnoreCase(name) =>
            layerParams = p
          case _ =>
        }
      }
    }

    assert(layerParams != null)
    layerParams
  }

  def getLossFunc(jsonAst: JValue): LossFuncParams = {
    val defaultLoss = jsonAst \ ParamKeys.defaultLossFunc match {
      case jast: JValue => Some(LossFuncParams(jast))
    }

    LossFuncParams(jsonAst \\ ParamKeys.lossFunc, defaultLoss)
  }

  def getFCLayer(jsonAst: JValue, inputLayer: Layer)(implicit graph: AngelGraph): FCLayer = {
    val defaultOpt = jsonAst \ ParamKeys.defaultOptimizer match {
      case JNothing => None
      case jast: JValue => Some(OptParams(jast))
    }

    val defaultTrans = jsonAst \ ParamKeys.defaultTransFunc match {
      case JNothing => None
      case jast: JValue => Some(TransFuncParams(jast))
    }

    val defaultLoss = jsonAst \ ParamKeys.defaultLossFunc match {
      case JNothing => None
      case jast: JValue => Some(LossFuncParams(jast))
    }

    var layerParams: FCLayerParams = null
    jsonAst \ ParamKeys.layers match {
      case layerList: JArray => layerList.arr.foreach { layer =>
        LayerParams(layer, defaultLoss, defaultTrans, defaultOpt) match {
          case p: FCLayerParams if p.layerType.equalsIgnoreCase("fclayer") =>
            layerParams = p
          case _ =>
        }
      }
    }

    assert(layerParams != null)
    val name = layerParams.name
    val outputDims = layerParams.outputDims
    val transFuncs = layerParams.transFuncs
    val optimizer = layerParams.optimizer.build()

    if (layerParams.outputDims.length == 1) {
      new FCLayer(name, outputDims.head, inputLayer, transFuncs.head.build(), optimizer)
    } else {
      var curName: String = s"${name}_0"
      var lastLayer: FCLayer = null
      var curInputLayer: Layer = inputLayer
      outputDims.zip(transFuncs).zipWithIndex.foreach { case ((outputDim, transFunc), idx) =>
        lastLayer = new FCLayer(curName, outputDim, curInputLayer, transFunc.build(), optimizer)

        curInputLayer = lastLayer
        curName = if (idx != layerParams.outputDims.length - 2) {
          s"${name}_${idx + 1}"
        } else {
          name
        }
      }

      lastLayer
    }
  }

  def parseJson(filename: String): JValue = {
    val sb: StringBuffer = new StringBuffer()
    for (line <- Source.fromFile(filename).getLines) {
      sb.append(line.trim)
    }

    parse(sb.toString)
  }
}
