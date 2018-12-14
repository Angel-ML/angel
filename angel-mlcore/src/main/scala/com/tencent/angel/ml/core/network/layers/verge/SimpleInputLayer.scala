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


import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.TransFunc
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.variable._
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{Callback, LayerKeys, MLException}
import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, Matrix}
import com.tencent.angel.ml.math2.utils.VectorUtils
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST
import org.json4s.JsonAST.JField
import org.json4s.JsonDSL._


class SimpleInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: Graph)
  extends InputLayer(name, outputDim)(graph) with Trainable with Serializable {
  graph.addTrainable(this)

  private val LOG = LogFactory.getLog(classOf[SimpleInputLayer])

  private val weight: MatVariable = graph.provider.getMatVariable(s"${name}_weight", outputDim,
    graph.indexRange, optimizer.numSlot, inIPLayer = true)
  private val bias: VecVariable = graph.provider.getVecVariable(s"${name}_bias", outputDim,
    optimizer.numSlot, inIPLayer = true)

  @transient var forward: Matrix = _ // dense
  // dense
  @transient var backward: Matrix = _ // dense
  // dense
  @transient var output: Matrix = _ // dense

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        // println(s"the status in SparseInputLayer($name)-calOutput is ${status.toString}")
        (weight, graph.valueType) match {
          case (_: BlasMatVariable, _) => // the shape of weight matrix is (inputDim, outputDim)
            forward = graph.placeHolder.getFeats.dot(weight).iadd(bias)
          case (_: MatVariable, "double") => // the shape of weight matrix is (outputDim, inputDim)
            forward = MFactory.denseDoubleMatrix(graph.placeHolder.getBatchSize, outputDim)
            (0 until outputDim).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
              forward.asInstanceOf[BlasDoubleMatrix].setCol(colId, col)
            }
          case (_: MatVariable, "float") =>
            forward = MFactory.denseFloatMatrix(graph.placeHolder.getBatchSize, outputDim)
            (0 until outputDim).foreach { colId =>
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getFloat(bias, colId))
              forward.asInstanceOf[BlasFloatMatrix].setCol(colId, col)
            }
        }

        output = transFunc(forward)
        status = STATUS.Forward
      case _ =>
    }
    val end = System.currentTimeMillis()
    // println(s"SparseInputLayer($name) calOutput Time=${end - start} ms")

    output
  }

  def calBackward(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        // println(s"the status in SparseInputLayer($name)-calBackward is ${status.toString}")
        val gradTemp = gatherGrad()
        backward = transFunc.calGrad(output, gradTemp)
        status = STATUS.Backward
      case _ =>
    }
    val end = System.currentTimeMillis()
    // println(s"SparseInputLayer($name) calBackward Time=${end - start} ms")

    backward
  }

  override def pullParams(epoch: Int): Unit = {
    // Note: weight is a row based matrix
    SharedConf.inputDataFormat match {
      case "dense" => // dense data
        weight.pullParams(epoch)
      case _ => // sparse data
        val indices = graph.placeHolder.getIndices
        weight.pullParams(epoch, indices)
    }

    bias.pullParams(epoch)
  }

  override def pushGradient(): Unit = {
    val start = System.currentTimeMillis()

    status match {
      case STATUS.Backward =>
        weight.pushGrads(graph.placeHolder.getFeats, backward)
        bias.pushGrads(backward, optimizer.lr)
        status = STATUS.Gradient
      case _ =>
    }

    val end = System.currentTimeMillis()
    // println(s"pushGradient Time = ${end - start} ms")
  }

  override def update[T](epoch: Int, batchSize: Int)(callback: Callback[T]): Callback[T] = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Gradient =>
        val future = weight.update[T](optimizer, epoch, batchSize)
        callback.setFuture(future)
        bias.update[T](optimizer, epoch, batchSize)
        status = STATUS.Update

        callback
      case _ => throw MLException("STATUS Error, please calculate Gradient first!")
    }
    // val end = System.currentTimeMillis()
    // println(s"update Time = ${end - start} ms")
  }


  override def init(taskFlag: Int): Unit = {
    weight.init(taskFlag, mean = 0.0, stddev = 0.000001)
    bias.init(taskFlag, mean = 0.0, stddev = 0.000001)
  }

  override def toString: String = {
    s"SimpleInputLayer name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def toJson(): JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.transFuncKey -> transFunc.toJson) ~
      (LayerKeys.optimizerKey -> optimizer.toJson)

    JField(name, layerJson)
  }
}
