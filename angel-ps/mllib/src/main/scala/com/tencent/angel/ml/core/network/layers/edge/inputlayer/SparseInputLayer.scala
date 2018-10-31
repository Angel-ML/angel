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

import java.util.concurrent.Future

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
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
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
  private val validIndexNum = SharedConf.modelSize

  private val weightCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
  weightCtx.setValidIndexNum(validIndexNum)
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

  override def pullParams(epoch: Int): Unit = {
    // Note: weight is a row based matrix
    val indices = graph.placeHolder.getIndices
    weight = PSMatrixUtils.getMatrixWithIndex(epoch, weightId, 0, outputDim, indices)
    bias = PSMatrixUtils.getRow(epoch, biasId, 0)
  }

  override def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    val normal = OptUtils.getNormal(sharedConf, graph)
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

//          var gradStr = ""
//          (0 until 10).foreach{ col =>
//            valueType match {
//              case "double" =>
//                gradStr += weightRowGrad.asInstanceOf[IntDoubleVector].get(col) + ","
//              case "float" =>
//                gradStr += weightRowGrad.asInstanceOf[IntFloatVector].get(col) + ","
//            }
//
//          }
//          LOG.info("gradient of " + graph.placeHolder.getBatchSize + " samples: " + gradStr)

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

  override def update(epoch: Int, batchSize: Int): Future[VoidResult] = {
    val start = System.currentTimeMillis()
    var result:Future[VoidResult] = null
    status match {
      case STATUS.Gradient =>
        result = optimizer.update(weightId, outputDim, epoch, batchSize)
        status = STATUS.Update
      case _ => throw new AngelException("STATUS Error, please calculate Gradient first!")
    }
    val end = System.currentTimeMillis()
    //    println(s"update Time = ${end - start} ms")
    result
  }

  override def init(taskflag: Int): Unit = {}

  override def toString: String = {
    s"SparseInputLayer name=$name outputDim=$outputDim optimizer=$optimizer"
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
}
