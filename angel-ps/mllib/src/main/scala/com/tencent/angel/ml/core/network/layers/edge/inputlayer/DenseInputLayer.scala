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


package com.tencent.angel.ml.core.network.layers.edge.inputlayer

import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.{AngelConf, MatrixConf}
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, Matrix}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.transfunc.TransFunc
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.{NetUtils, PSMatrixUtils}
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory


class DenseInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable {
  val LOG = LogFactory.getLog(classOf[DenseInputLayer])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf

  val modelType: RowType = SharedConf.denseModelType
  val blockSize: Int = SharedConf.blockSize

  private val multiplier = OptUtils.getOptMultiplier(optimizer)
  private val inputDim = SharedConf.indexRange.toInt
  private val psRows: Int = multiplier
  private val psCols = inputDim * outputDim
  val psBlockSize: Long = (blockSize + psRows - 1) / psRows

  private val weightCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
  private val biasCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_bias", 1, outputDim, modelType)

  graph.addMatrixCtx(weightCtx)
  graph.addMatrixCtx(biasCtx)

  lazy val weightId: Int = PSMatrixUtils.getMatrixId(s"${name}_weight")
  lazy val biasId: Int = PSMatrixUtils.getMatrixId(s"${name}_bias")

  @transient var forward: Matrix = _
  @transient var backward: Matrix = _
  @transient var output: Matrix = _

  @transient var weight: Matrix = _
  @transient var bias: Vector = _

  override def calOutput(): Matrix = {
    status match {
      case STATUS.Null =>
        forward = SharedConf.inputDataFormat match {
          case "dense" => graph.placeHolder.getFeats.dot(weight).iadd(bias)
          case _ =>
            NetUtils.valueType(modelType) match {
              case "double" =>
                val temp = MFactory.denseDoubleMatrix(graph.placeHolder.getBatchSize, outputDim)
                (0 until outputDim).foreach { colId =>
                  val col = graph.placeHolder.getFeats.dot(weight.asInstanceOf[BlasDoubleMatrix].getCol(colId))
                  temp.setCol(colId, col.iadd(VectorUtils.getDouble(bias, colId)))
                }
                temp
              case "float" =>
                val temp = MFactory.denseFloatMatrix(graph.placeHolder.getBatchSize, outputDim)
                (0 until outputDim).foreach { colId =>
                  val col = graph.placeHolder.getFeats.dot(weight.asInstanceOf[BlasFloatMatrix].getCol(colId))
                  temp.setCol(colId, col.iadd(VectorUtils.getFloat(bias, colId)))
                }
                temp
            }
        }

        output = transFunc(forward)
        status = STATUS.Forward
      case _ =>
    }
    output
  }

  override def calBackward(): Matrix = {
    status match {
      case STATUS.Forward =>
        val gradTemp = gatherGrad()
        backward = transFunc.calGrad(output, gradTemp)
        status = STATUS.Backward
      case _ =>
    }

    backward
  }

  override def pullParams(): Unit = {
    weight = PSMatrixUtils.getRowAsMatrix(weightId, 0, inputDim, outputDim)
    bias = PSMatrixUtils.getRow(biasId, 0)
  }

  override def pushGradient(): Unit = {
    val normal = graph.placeHolder.getBatchSize * graph.taskNum

    status match {
      case STATUS.Backward =>
        SharedConf.inputDataFormat match {
          case "dense" =>
            val weightGrad: Matrix = Ufuncs.dot(graph.placeHolder.getFeats, true, backward, false)
              .idiv(normal)
            PSMatrixUtils.incrementRowByMatrix(weightId, multiplier - 1, weightGrad)
          case _ =>
            NetUtils.valueType(modelType) match {
              case "double" =>
                val weightGrad = MFactory.denseDoubleMatrix(inputDim, outputDim)
                (0 until outputDim).foreach { colId =>
                  val colGrad = graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasDoubleMatrix].getCol(colId))
                    .idiv(normal).asInstanceOf[IntDoubleVector]
                  weightGrad.setCol(colId, colGrad)
                }
                PSMatrixUtils.incrementRowByMatrix(weightId, multiplier - 1, weightGrad)
              case "float" =>
                val weightGrad = MFactory.denseFloatMatrix(inputDim, outputDim)
                (0 until outputDim).foreach { colId =>
                  val colGrad = graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasFloatMatrix].getCol(colId))
                    .idiv(normal).asInstanceOf[IntFloatVector]
                  weightGrad.setCol(colId, colGrad)
                }
                PSMatrixUtils.incrementRowByMatrix(weightId, multiplier - 1, weightGrad)
            }
        }

        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.lr / graph.taskNum))

        status = STATUS.Gradient
      case _ =>
    }
  }

  override def update(epoch: Int = 0): Unit = {
    status match {
      case STATUS.Gradient =>
        optimizer.update(weightId, 1, epoch)
        status = STATUS.Update
      case _ => throw new AngelException("STATUS Error, please calculate Gradient frist!")
    }
  }

  override def init(taskflag: Int, initIndexVector: Vector = null): Unit = {
    val bound: Double = 0.00001
    if (taskflag == 0) {
      val randFunc = new RandomNormal(weightId, 0, 0.0, bound)
      PSAgentContext.get().getUserRequestAdapter.update(randFunc)
    }
  }

  override def toString: String = {
    return s"DenseInputLayer name=${name} outputDim=$outputDim optimizer=${optimizer}"
  }

  override def loadParams(client: AngelClient): Unit = {
    SharedConf.actionType().toLowerCase match {
      case "train" =>
        weightCtx.set(MatrixConf.MATRIX_SAVE_PATH, sharedConf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))
        biasCtx.set(MatrixConf.MATRIX_SAVE_PATH, sharedConf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))
      case "inctrain" =>
        weightCtx.set(MatrixConf.MATRIX_SAVE_PATH, sharedConf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))
        biasCtx.set(MatrixConf.MATRIX_SAVE_PATH, sharedConf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))
        weightCtx.set(MatrixConf.MATRIX_LOAD_PATH, sharedConf.get(AngelConf.ANGEL_LOAD_MODEL_PATH))
        biasCtx.set(MatrixConf.MATRIX_LOAD_PATH, sharedConf.get(AngelConf.ANGEL_LOAD_MODEL_PATH))

        weightCtx.init(client.getConf)
        biasCtx.init(client.getConf)
      case "predict" =>
        weightCtx.set(MatrixConf.MATRIX_LOAD_PATH, sharedConf.get(AngelConf.ANGEL_LOAD_MODEL_PATH))
        biasCtx.set(MatrixConf.MATRIX_LOAD_PATH, sharedConf.get(AngelConf.ANGEL_LOAD_MODEL_PATH))

        weightCtx.init(client.getConf)
        biasCtx.init(client.getConf)
    }

    client.addMatrix(weightCtx)
    client.addMatrix(biasCtx)
  }

  override def saveParams(saveContext: ModelSaveContext): Unit = {
    SharedConf.actionType().toLowerCase match {
      case "train" | "inctrain" =>
        val weightMCS: MatrixSaveContext = new MatrixSaveContext(weightCtx.getName)
        val biasMCS: MatrixSaveContext = new MatrixSaveContext(biasCtx.getName)
        saveContext.addMatrix(weightMCS)
        saveContext.addMatrix(biasMCS)
      case _ =>
    }
  }

}
