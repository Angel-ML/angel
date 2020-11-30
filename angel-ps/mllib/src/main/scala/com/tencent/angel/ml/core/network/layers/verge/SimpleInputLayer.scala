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
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.transfunc.TransFunc
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.paramsutils.ParamKeys
import com.tencent.angel.ml.core.utils.{NetUtils, PSMatrixUtils}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST._
import org.json4s.JsonDSL._


class SimpleInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable with Serializable {
  val LOG = LogFactory.getLog(classOf[SimpleInputLayer])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf

  val parallel = sharedConf.get(MLConf.ML_MATRIX_DOT_USE_PARALLEL_EXECUTOR).toBoolean
  val modelType: RowType = SharedConf.modelType
  val valueType: String = SharedConf.valueType()
  val inputDataFormat: String = SharedConf.inputDataFormat
  val mode = SharedConf.runningMode()
  val modelsize = SharedConf.modelSize


  private val numSlot = OptUtils.getSlotNum(optimizer)

  private val weightCtx: MatrixContext = (inputDataFormat, NetUtils.storageType(modelType)) match {
    case ("dense", "dense" | "component_dense") => // dense data, dense model
      // in this condition, all the parameters are stored in one row
      val psRows: Int = numSlot + 1
      val psCols = SharedConf.indexRange * outputDim
      PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
    // in this condition, the shape of weight matrix is (inputDim, outputDim)
    // and inputDim = SharedConf.indexRange
    case ("libsvm" | "dummy", "dense" | "component_dense") => // sparse data, dense model
      val psRows: Int = outputDim * (numSlot + 1)
      val psCols = SharedConf.indexRange
      PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
    // in this condition, the shape of weight matrix is (outputDim, inputDim)
    // and inputDim = SharedConf.indexRange
    case ("libsvm" | "dummy", "sparse" | "component_sparse") => // sparse data, sparse model
      val psRows: Int = outputDim * (numSlot + 1)
      val psCols = SharedConf.indexRange
      val wCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
      // in this condition, the shape of weight matrix is (outputDim, inputDim)
      // and inputDim = SharedConf.indexRange
      wCtx.setValidIndexNum(modelsize)
      wCtx
    case _ => // dense data, sparse model
      throw new AngelException("Dense data, sparse model, pls. change model to dense")
  }

  private val biasCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_bias", 1, outputDim, SharedConf.denseModelType)
  graph.addMatrixCtx(weightCtx)
  graph.addMatrixCtx(biasCtx)

  lazy val weightId: Int = PSMatrixUtils.getMatrixId(s"${name}_weight")
  lazy val biasId: Int = PSMatrixUtils.getMatrixId(s"${name}_bias")

  @transient var forward: Matrix = _ // dense
  // dense
  @transient var backward: Matrix = _ // dense
  // dense
  @transient var output: Matrix = _ // dense

  @transient var weight: Matrix = _
  // ??
  @transient var bias: Vector = _ // dense

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        // println(s"the status in SparseInputLayer($name)-calOutput is ${status.toString}")
        (inputDataFormat, valueType) match {
          case ("dense", "double" | "float") => // the shape of weight matrix is (inputDim, outputDim)
            forward = graph.placeHolder.getFeats.dot(weight, parallel).iadd(bias)
          case ("libsvm" | "dummy", "double") => // the shape of weight matrix is (outputDim, inputDim)
            forward = MFactory.denseDoubleMatrix(graph.placeHolder.getBatchSize, outputDim)
            (0 until outputDim).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
              forward.asInstanceOf[BlasDoubleMatrix].setCol(colId, col)
            }
          case ("libsvm" | "dummy", "float") =>
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
    (inputDataFormat, NetUtils.storageType(modelType)) match {
      case ("dense", "dense" | "component_dense") => // dense data, dense model
        // the shape of weight matrix is (inputDim, outputDim)
        weight = PSMatrixUtils.getRowAsMatrix(epoch, weightId, 0, SharedConf.indexRange.toInt, outputDim)
      case ("libsvm" | "dummy", "dense" | "component_dense") => // sparse data, dense model
        val indices = graph.placeHolder.getIndices
        // the shape of weight matrix is (outputDim, inputDim)
        weight = PSMatrixUtils.getMatrixWithIndex(1, weightId, 0, outputDim, indices)
      case ("libsvm" | "dummy", "sparse" | "component_sparse") => // sparse data, sparse model
        val indices = graph.placeHolder.getIndices
        // the shape of weight matrix is (outputDim, inputDim)
        // if epoch = 0, initAndGet(), else get()
        weight = PSMatrixUtils.getMatrixWithIndex(epoch, weightId, 0, outputDim, indices)
      case _ => // dense data, sparse model
        throw new AngelException("Dense data, sparse model, pls. change model to dense")
    }
    bias = PSMatrixUtils.getRow(epoch, biasId, 0)
  }

  override def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    val normal = 1.0 / OptUtils.getNormal(mode, graph)

    status match {
      case STATUS.Backward =>
        (inputDataFormat, NetUtils.storageType(modelType)) match {
          case ("dense", "dense" | "component_dense") => // dense data, dense model
            val weightGrad: Matrix = Ufuncs.dot(graph.placeHolder.getFeats, true, backward, false, parallel)
              .imul(normal)
            PSMatrixUtils.incrementRowByMatrix(weightId, numSlot, weightGrad)
          case _ => // sparse data, dense or sparse model, note: dense data, sparse model is not allowed
            val vectors = (0 until outputDim).toArray.map { colId =>
              val weightRowGrad = valueType match {
                case "double" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasDoubleMatrix].getCol(colId))
                    .imul(normal)
                case "float" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasFloatMatrix].getCol(colId))
                    .imul(normal)
              }

              weightRowGrad.setMatrixId(weight.getMatrixId)
              weightRowGrad.setRowId(outputDim * numSlot + colId)
              weightRowGrad.setClock(weight.getClock)

              weightRowGrad
            }

            PSMatrixUtils.incrementRows(weightId, vectors.map(_.getRowId), vectors)
        }


        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.getLR / graph.taskNum))

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
        (inputDataFormat, NetUtils.storageType(modelType)) match {
          case ("dense", "dense" | "component_dense") => // dense data, dense model
            result = optimizer.update(weightId, 1, epoch, batchSize)
          case _ =>
            result = optimizer.update(weightId, outputDim, epoch, batchSize)
        }
        status = STATUS.Update
      case _ => throw new AngelException("STATUS Error, please calculate Gradient first!")
    }
    val end = System.currentTimeMillis()
    // println(s"update Time = ${end - start} ms")
    result
  }

  override def init(taskflag: Int): Unit = {
    if (taskflag == 0) {
      val bound = 0.0001
      (inputDataFormat, NetUtils.storageType(modelType)) match {
        case ("dense", "dense" | "component_dense") => // dense data, dense model
          val randFunc = new RandomNormal(weightId, 0, 1, 0.0, bound)
          PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
        case ("libsvm" | "dummy", "dense" | "component_dense") => // sparse data, dense model
          val randFunc = new RandomNormal(weightId, 0, outputDim, 0.0, bound)
          PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
        case _ => // sparse model, no need to initial, use iniAndGet instead
      }
    }
  }

  override def toString: String = {
    s"SimpleInputLayer name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def loadParams(loadContext: ModelLoadContext): Unit = {
    loadContext.addMatrix(new MatrixLoadContext(weightCtx.getName))
    loadContext.addMatrix(new MatrixLoadContext(biasCtx.getName))
  }

  override def saveParams(saveContext: ModelSaveContext): Unit = {
    val outputFormat = SharedConf.sparseInputLayerMatrixOutputFormat
    val weightMCS: MatrixSaveContext = new MatrixSaveContext(weightCtx.getName, outputFormat)
    val biasMCS: MatrixSaveContext = new MatrixSaveContext(biasCtx.getName, outputFormat)
    weightMCS.addIndices((0 until outputDim).toArray)
    saveContext.addMatrix(weightMCS)
    saveContext.addMatrix(biasMCS)
  }

  override def toJson: JObject = {
    (ParamKeys.name -> name) ~
      (ParamKeys.typeName -> s"${this.getClass.getSimpleName}") ~
      (ParamKeys.outputDim -> outputDim) ~
      (ParamKeys.transFunc -> transFunc.toJson) ~
      (ParamKeys.optimizer -> optimizer.toJson)
  }
}
