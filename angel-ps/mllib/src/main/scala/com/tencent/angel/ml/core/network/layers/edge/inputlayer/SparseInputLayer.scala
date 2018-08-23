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
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.math2.matrix._
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


class SparseInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable with Serializable {
  val LOG = LogFactory.getLog(classOf[SparseInputLayer])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf

  val modelType: RowType = SharedConf.modelType
  val valueType: String = SharedConf.valueType()

  private val multiplier = OptUtils.getOptMultiplier(optimizer)
  private val psRows: Int = outputDim * multiplier
  private val psCols = SharedConf.indexRange

  private val weightCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
  private val biasCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_bias", 1, outputDim, SharedConf.denseModelType)
  graph.addMatrixCtx(weightCtx)
  graph.addMatrixCtx(biasCtx)

  lazy val weightId: Int = PSMatrixUtils.getMatrixId(s"${name}_weight")
  lazy val biasId: Int = PSMatrixUtils.getMatrixId(s"${name}_bias")

  @transient var forward: Matrix = _
  // dense
  @transient var backward: Matrix = _
  // dense
  @transient var output: Matrix = _ // dense

  @transient var weight: Matrix = _
  // ??
  @transient var bias: Vector = _ // dense

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        //        println(s"the status in SparseInputLayer($name)-calOutput is ${status.toString}")
        valueType match {
          case "double" =>
            forward = MFactory.denseDoubleMatrix(graph.placeHolder.getBatchSize, outputDim)
            (0 until outputDim).foreach { colId =>
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
              forward.asInstanceOf[BlasDoubleMatrix].setCol(colId, col)
            }
          case "float" =>
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
    //    println(s"SparseInputLayer($name) calOutput Time=${end - start} ms")

    output
  }

  def calBackward(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        //        println(s"the status in SparseInputLayer($name)-calBackward is ${status.toString}")
        val gradTemp = gatherGrad()
        backward = transFunc.calGrad(output, gradTemp)
        status = STATUS.Backward
      case _ =>
    }
    val end = System.currentTimeMillis()
    //    println(s"SparseInputLayer($name) calBackward Time=${end - start} ms")

    backward
  }

  override def pullParams(): Unit = {
    // Note: weight is a row based matrix
    //    LOG.error("Sparse Input Layer pull")
    val indices = graph.placeHolder.getIndices
    weight = PSMatrixUtils.getMatrixWithIndex(weightId, 0, outputDim, indices)
    bias = PSMatrixUtils.getRow(biasId, 0)

  }

  override def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    val normal = graph.placeHolder.getBatchSize * graph.taskNum
    val rowIds = new Array[Int](outputDim)
    val vectors = new Array[Vector](outputDim)

    status match {
      case STATUS.Backward =>
        (0 until outputDim).foreach { rowId =>
          val weightRowGrad = valueType match {
            case "double" =>
              graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasDoubleMatrix].getCol(rowId))
                .idiv(normal)
            case "float" =>
              graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasFloatMatrix].getCol(rowId))
                .idiv(normal)
          }

          weightRowGrad.setMatrixId(weight.getMatrixId)
          weightRowGrad.setRowId(outputDim * (multiplier - 1) + rowId)
          weightRowGrad.setClock(weight.getClock)
          rowIds(rowId) = weightRowGrad.getRowId
          vectors(rowId) = weightRowGrad
          // PSMatrixUtils.incrementRow(weightId, outputDim * (multiplier-1) + rowId, weightRowGrad)
        }
        PSMatrixUtils.incrementRows(weightId, rowIds, vectors)

        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.lr / graph.taskNum))

        status = STATUS.Gradient
      case _ =>
    }
    val end = System.currentTimeMillis()
    //    println(s"pushGradient Time = ${end - start} ms")
  }

  override def update(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Gradient =>
        optimizer.update(weightId, outputDim, epoch)
        status = STATUS.Update
      case _ => throw new AngelException("STATUS Error, please calculate Gradient first!")
    }
    val end = System.currentTimeMillis()
    //    println(s"update Time = ${end - start} ms")
  }

  override def init(taskflag: Int, indexVector: Vector): Unit = {
    val valueType: String = SharedConf.valueType()
    val bound: Double = 0.00001 / graph.taskNum

    if (indexVector == null) {
      if (taskflag == 0) {
        val randFunc = new RandomNormal(weightId, 0, outputDim, 0.0, bound)
        PSAgentContext.get().getUserRequestAdapter.update(randFunc)
      }
    } else {
      (indexVector, valueType) match {
        case (idx: IntIntVector, "double") =>
          (0 until outputDim).toArray.foreach { i =>
            val keys = idx.getStorage.getValues.clone()
            val values = keys.map { _ => Math.random() * bound - bound / 2 }
            val randomVector = VFactory.sortedDoubleVector(psCols.toInt, keys, values)
            PSMatrixUtils.incrementRow(weightId, i, randomVector)
          }
        case (idx: IntIntVector, "float") =>
          (0 until outputDim).toArray.foreach { i =>
            val keys = idx.getStorage.getValues.clone()
            val values = keys.map { _ => (Math.random() * bound - bound / 2).toFloat }
            val randomVector = VFactory.sortedFloatVector(psCols.toInt, keys, values)
            PSMatrixUtils.incrementRow(weightId, i, randomVector)
          }
        case (idx: IntLongVector, "double") =>
          (0 until outputDim).toArray.foreach { i =>
            val keys = idx.getStorage.getValues.clone()
            val values = keys.map { _ => Math.random() * bound - bound / 2 }
            val randomVector = VFactory.sortedLongKeyDoubleVector(psCols, keys.length, keys, values)
            PSMatrixUtils.incrementRow(weightId, i, randomVector)
          }
        case (idx: IntLongVector, "float") =>
          (0 until outputDim).toArray.foreach { i =>
            val keys = idx.getStorage.getValues.clone()
            val values = keys.map { _ => (Math.random() * bound - bound / 2).toFloat }
            val randomVector = VFactory.sortedLongKeyFloatVector(psCols, keys.length, keys, values)
            PSMatrixUtils.incrementRow(weightId, i, randomVector)
          }
      }
    }
  }

  override def toString: String = {
    s"SparseInputLayer name=$name outputDim=$outputDim optimizer=$optimizer"
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
        weightMCS.addIndices((0 until outputDim).toArray)
        saveContext.addMatrix(weightMCS)
        saveContext.addMatrix(biasMCS)
      case _ =>
    }
  }
}
