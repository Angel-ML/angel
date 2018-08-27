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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.network.layers.{AngelGraph, Layer}
import com.tencent.angel.ml.core.network.layers.edge.inputlayer.{DenseInputLayer, Embedding, SparseInputLayer}
import com.tencent.angel.ml.core.network.layers.edge.losslayer.{SimpleLossLayer, SoftmaxLossLayer}
import com.tencent.angel.ml.core.network.layers.join.{ConcatLayer, DotPooling, MulPooling, SumPooling}
import com.tencent.angel.ml.core.network.layers.linear._
import org.json4s.{DefaultFormats, JArray, JInt, JNothing, JValue}

import scala.collection.mutable

abstract class LayerParams(val name: String, val layerType: String) {
  def build(params: Map[String, LayerParams])(implicit layers: mutable.HashMap[String, Layer], graph: AngelGraph): Layer
}

object LayerParams {
  implicit val formats = DefaultFormats
  val denseInput: String = classOf[DenseInputLayer].getSimpleName.toLowerCase
  val sparseInput: String = classOf[SparseInputLayer].getSimpleName.toLowerCase
  val embedding: String = classOf[Embedding].getSimpleName.toLowerCase
  val simpleLoss: String = classOf[SimpleLossLayer].getSimpleName.toLowerCase
  val softmaxLoss: String = classOf[SoftmaxLossLayer].getSimpleName.toLowerCase
  val concat: String = classOf[ConcatLayer].getSimpleName.toLowerCase
  val dotPooling: String = classOf[DotPooling].getSimpleName.toLowerCase
  val mulPooling: String = classOf[MulPooling].getSimpleName.toLowerCase
  val sumPooling: String = classOf[SumPooling].getSimpleName.toLowerCase
  val biInnerCross: String = classOf[BiInnerCross].getSimpleName.toLowerCase
  val biInnerSumCross: String = classOf[BiInnerSumCross].getSimpleName.toLowerCase
  val biInteractionCross: String = classOf[BiInteractionCross].getSimpleName.toLowerCase
  val biOuterCross: String = classOf[BiOutterCross].getSimpleName.toLowerCase
  val fcLayer: String = classOf[FCLayer].getSimpleName.toLowerCase

  def apply(json: JValue,
            defaultLossFunc: Option[LossFuncParams],
            defaultTransFunc: Option[TransFuncParams],
            defaultOpt: Option[OptParams]): LayerParams = {
    val name: String = (json \ ParamKeys.name).extract[String].trim
    val layerType: String = json \ ParamKeys.typeName match {
      case JNothing => throw new AngelException("type in Layer is not set!")
      case lt: JValue => lt.extract[String].trim
    }

    layerType.toLowerCase match {
      case `denseInput` | `sparseInput` =>
        val outputDim: Int = json \ ParamKeys.outputDim match {
          case JNothing => throw new AngelException("outputDim in InputLayer is not set!")
          case v: JInt => v.extract[Int]
        }

        val transParams = TransFuncParams(json \ ParamKeys.transFunc, defaultTransFunc)
        val optParams = OptParams(json \ ParamKeys.optimizer, defaultOpt)
        InputLayerParams(name, layerType, outputDim, transParams, optParams)
      case `embedding` =>
        val outputDim: Int = json \ ParamKeys.outputDim match {
          case JNothing => throw new AngelException("outputdim in EmbeddingLayer is not set!")
          case v: JInt => v.extract[Int]
        }

        val numFactors = json \ ParamKeys.numFactors match {
          case JNothing => throw new AngelException("numfactors in EmbeddingLayer is not set!")
          case v: JInt => v.extract[Int]
        }

        val optParams = OptParams(json \ ParamKeys.optimizer, defaultOpt)
        EmbeddingParams(name, layerType, outputDim, numFactors, optParams)
      case `simpleLoss` | `softmaxLoss` =>
        val inputLayer = json \ ParamKeys.inputLayer match {
          case JNothing => throw new AngelException("inputlayer in LossLayer is not set!")
          case il: JValue => il.extract[String]
        }

        val lossParams = LossFuncParams(json \ ParamKeys.lossFunc, defaultLossFunc)
        LossLayerParams(name, layerType, inputLayer, lossParams)
      case `concat` | `dotPooling` | `mulPooling` | `sumPooling` =>
        val outputDim = json \ ParamKeys.outputDim match {
          case JNothing => throw new AngelException("outputDim in JoinLayer is not set!")
          case opd: JValue => opd.extract[Int]
        }

        val inputLayers = json \ ParamKeys.inputLayers match {
          case JNothing => throw new AngelException("inputLayers in JoinLayer is not set!")
          case ils: JValue => ils.extract[Array[String]]
        }

        JoinLayerParams(name, layerType, outputDim, inputLayers)
      case `biInnerCross` | `biInnerSumCross` | `biInteractionCross` | `biOuterCross` =>
        val outputDim: Option[Int] = json \ ParamKeys.outputDim match {
          case JNothing => None
          case v: JInt => Some(v.extract[Int])
        }

        val inputLayer = json \ ParamKeys.inputLayer match {
          case JNothing => throw new AngelException("inputLayer in CrossLayer is not set!")
          case il: JValue => il.extract[String]
        }

        CrossLayerParams(name, layerType, outputDim, inputLayer)
      case `fcLayer` =>
        val outputDim = json \ ParamKeys.outputDims match {
          case JNothing => throw new AngelException("outputDims in FCLayer is not set!")
          case opd: JValue => opd.extract[Array[Int]]
        }

        val inputLayer = json \ ParamKeys.inputLayer match {
          case JNothing => throw new AngelException("inputLayer in FCLayer is not set!")
          case il: JValue => il.extract[String]
        }

        val transParams = json \ ParamKeys.transFuncs match {
          case JNothing => throw new AngelException("transFuncs in FCLayer is not set!")
          case transArr: JArray =>
            transArr.arr.map { jast =>
              TransFuncParams(jast, defaultTransFunc)
            }.toArray
        }

        val optParams = OptParams(json \ ParamKeys.optimizer, defaultOpt)
        FCLayerParams(name, layerType, outputDim, inputLayer, transParams, optParams)
    }
  }
}

case class InputLayerParams(override val name: String,
                            override val layerType: String,
                            outputDim: Int,
                            transFunc: TransFuncParams,
                            optimizer: OptParams) extends LayerParams(name, layerType) {
  override def build(params: Map[String, LayerParams])(implicit layers: mutable.HashMap[String, Layer], graph: AngelGraph): Layer = {
    if (layers.contains(name)) {
      layers(name)
    } else {
      import LayerParams._

      val layer = layerType.trim.toLowerCase match {
        case `sparseInput` => new SparseInputLayer(name, outputDim, transFunc.build(), optimizer.build())
        case `denseInput` => new DenseInputLayer(name, outputDim, transFunc.build(), optimizer.build())
      }

      layers.put(name, layer)
      layer
    }
  }
}

case class EmbeddingParams(override val name: String,
                           override val layerType: String,
                           outputDim: Int,
                           numFactors: Int,
                           optimizer: OptParams) extends LayerParams(name, layerType) {
  override def build(params: Map[String, LayerParams])(implicit layers: mutable.HashMap[String, Layer], graph: AngelGraph): Layer = {
    if (layers.contains(name)) {
      layers(name)
    } else {
      val layer = new Embedding(name, outputDim, numFactors, optimizer.build())
      layers.put(name, layer)
      layer
    }
  }
}

case class LossLayerParams(override val name: String,
                           override val layerType: String,
                           inputLayer: String,
                           lossFunc: LossFuncParams) extends LayerParams(name, layerType) {
  override def build(params: Map[String, LayerParams])(implicit layers: mutable.HashMap[String, Layer], graph: AngelGraph): Layer = {
    if (layers.contains(name)) {
      layers(name)
    } else {
      import LayerParams._

      val layer = layerType.trim.toLowerCase match {
        case `simpleLoss` => new SimpleLossLayer(name, params(inputLayer).build(params), lossFunc.build())
        case `softmaxLoss` => new SoftmaxLossLayer(name, params(inputLayer).build(params), lossFunc.build())
      }

      layers.put(name, layer)
      layer
    }
  }
}

case class JoinLayerParams(override val name: String,
                           override val layerType: String,
                           outputDim: Int,
                           inputLayers: Array[String]) extends LayerParams(name, layerType) {
  override def build(params: Map[String, LayerParams])(implicit layers: mutable.HashMap[String, Layer], graph: AngelGraph): Layer = {
    if (layers.contains(name)) {
      layers(name)
    } else {
      import LayerParams._
      val layer = layerType.trim.toLowerCase match {
        case `concat` => new ConcatLayer(name, outputDim, inputLayers.map(layer => params(layer).build(params)))
        case `dotPooling` => new DotPooling(name, outputDim, inputLayers.map(layer => params(layer).build(params)))
        case `mulPooling` => new MulPooling(name, outputDim, inputLayers.map(layer => params(layer).build(params)))
        case `sumPooling` => new SumPooling(name, outputDim, inputLayers.map(layer => params(layer).build(params)))
      }
      layers.put(name, layer)
      layer
    }
  }
}

case class CrossLayerParams(override val name: String,
                            override val layerType: String,
                            outputDim: Option[Int],
                            inputLayer: String) extends LayerParams(name, layerType) {
  override def build(params: Map[String, LayerParams])(implicit layers: mutable.HashMap[String, Layer], graph: AngelGraph): Layer = {
    if (layers.contains(name)) {
      layers(name)
    } else {
      import LayerParams._
      val layer = layerType.trim.toLowerCase match {
        case `biInnerCross` => new BiInnerCross(name, outputDim.get, params(inputLayer).build(params))
        case `biInnerSumCross` => new BiInnerSumCross(name, params(inputLayer).build(params))
        case `biInteractionCross` => new BiInteractionCross(name, outputDim.get, params(inputLayer).build(params))
      }

      layers.put(name, layer)
      layer
    }
  }
}


case class FCLayerParams(override val name: String,
                         override val layerType: String,
                         outputDims: Array[Int],
                         inputLayer: String,
                         transFuncs: Array[TransFuncParams],
                         optimizer: OptParams) extends LayerParams(name, layerType) {
  override def build(params: Map[String, LayerParams])(implicit layers: mutable.HashMap[String, Layer], graph: AngelGraph): Layer = {
    if (layers.contains(name)) {
      layers(name)
    } else {
      if (outputDims.length == 1) {
        new FCLayer(name, outputDims.head, params(inputLayer).build(params), transFuncs.head.build(), optimizer.build())
      } else {
        var curInputLayer: String = inputLayer
        var curName: String = s"${name}_0"
        var lastLayer: Layer = params(curInputLayer).build(params)
        outputDims.zip(transFuncs).zipWithIndex.foreach { case ((outputDim, transFunc), idx) =>
          lastLayer = new FCLayer(curName, outputDim, lastLayer, transFunc.build(), optimizer.build())
          layers.put(curName, lastLayer)

          curInputLayer = curName
          curName = if (idx != outputDims.length - 2) {
            s"${name}_${idx + 1}"
          } else {
            name
          }
        }
        lastLayer.asInstanceOf[FCLayer]
      }
    }
  }
}
