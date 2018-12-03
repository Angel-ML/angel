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

import java.util.concurrent.Future

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.transfunc.TransFunc
import com.tencent.angel.ml.core.network.variable.MatVariable.MatrixType
import com.tencent.angel.ml.core.network.variable._
import com.tencent.angel.ml.core.network.variable.Variable.Location
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.RowTypeUtils
import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import org.apache.commons.logging.LogFactory


class SimpleInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: Graph)
  extends InputLayer(name, outputDim)(graph) with Trainable with Serializable {
  val LOG = LogFactory.getLog(classOf[SimpleInputLayer])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf
  val modelType: RowType = SharedConf.modelType
  val location: Location.Location = Location.PS

  private val numSlot = OptUtils.getSlotNum(optimizer)
  private val weight = (SharedConf.inputDataFormat, RowTypeUtils.storageType(modelType)) match {
    case ("dense", "dense" | "component_dense") => // dense data, dense model
      Variable.getMatrix(s"${this.getClass.getSimpleName}_weight", outputDim, SharedConf.indexRange,
        numSlot, modelType, MatrixType.Blas, location)
    case ("libsvm" | "dummy", "dense" | "component_dense") => // sparse data, dense model
      Variable.getMatrix(s"${this.getClass.getSimpleName}_weight", outputDim, SharedConf.indexRange,
        numSlot, modelType, MatrixType.Common, location)
    case ("libsvm" | "dummy", "sparse" | "component_sparse") => // sparse data, sparse model
      Variable.getMatrix(s"${this.getClass.getSimpleName}_weight", outputDim, SharedConf.indexRange,
        SharedConf.modelSize, numSlot, modelType, MatrixType.Common, location)
    case _ => // dense data, sparse model
      throw new AngelException("Dense data, sparse model, pls. change model to dense")
  }
  private val bias = Variable.getVector(s"${this.getClass.getSimpleName}_bias", outputDim,
    SharedConf.denseModelType, location)


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
        weight match {
          case _: PSBlasMatVariable => // the shape of weight matrix is (inputDim, outputDim)
            forward = graph.placeHolder.getFeats.dot(weight).iadd(bias)
          case w: PSMatVariable if w.valueType == "double" => // the shape of weight matrix is (outputDim, inputDim)
            forward = MFactory.denseDoubleMatrix(graph.placeHolder.getBatchSize, outputDim)
            (0 until outputDim).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
              forward.asInstanceOf[BlasDoubleMatrix].setCol(colId, col)
            }
          case w: PSMatVariable if w.valueType == "float" =>
            forward = MFactory.denseFloatMatrix(graph.placeHolder.getBatchSize, outputDim)
            (0 until outputDim).foreach { colId =>
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getFloat(bias, colId))
              forward.asInstanceOf[BlasFloatMatrix].setCol(colId, col)
            }
          case _: LocalBlasMatVariable => // the shape of weight matrix is (inputDim, outputDim)
            forward = graph.placeHolder.getFeats.dot(weight).iadd(bias)
          case w: LocalMatVariable if w.valueType == "double" => // the shape of weight matrix is (outputDim, inputDim)
            forward = MFactory.denseDoubleMatrix(graph.placeHolder.getBatchSize, outputDim)
            (0 until outputDim).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
              forward.asInstanceOf[BlasDoubleMatrix].setCol(colId, col)
            }
          case w: LocalMatVariable if w.valueType == "float" =>
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
      case _ => // dense data, sparse model
        throw new AngelException("Dense data, sparse model, pls. change model to dense")
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

  override def update(epoch: Int, batchSize: Int): Future[VoidResult] = {
    val start = System.currentTimeMillis()
    var result: Future[VoidResult] = null
    status match {
      case STATUS.Gradient =>
        result = weight.update(optimizer, epoch, batchSize)
        bias.update(optimizer, epoch, batchSize)
        status = STATUS.Update
      case _ => throw new AngelException("STATUS Error, please calculate Gradient first!")
    }
    val end = System.currentTimeMillis()
    // println(s"update Time = ${end - start} ms")
    result
  }

  override def init(taskFlag: Int): Unit = {
    weight.init(taskFlag, mean = 0.0, stddev = 0.000001)
    bias.init(taskFlag, mean = 0.0, stddev = 0.000001)
  }

  override def toString: String = {
    s"SimpleInputLayer name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def loadParams(loadContext: ModelLoadContext): Unit = {
    weight.loadParams(loadContext)
    bias.loadParams(loadContext)
  }

  override def saveParams(saveContext: ModelSaveContext): Unit = {
    weight.saveParams(saveContext)
    bias.saveParams(saveContext)
  }
}
