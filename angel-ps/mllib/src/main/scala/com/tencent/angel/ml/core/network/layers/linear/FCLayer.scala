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


package com.tencent.angel.ml.core.network.layers.linear


import java.util.concurrent.Future

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.layers.verge.Embedding
import com.tencent.angel.ml.core.network.transfunc.TransFunc
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.core.utils.paramsutils.ParamKeys
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.MatrixUtils
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

class FCLayer(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, override val optimizer: Optimizer
             )(implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) with Trainable {
  val LOG = LogFactory.getLog(classOf[FCLayer])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf
  val parallel = sharedConf.get(MLConf.ML_MATRIX_DOT_USE_PARALLEL_EXECUTOR).toBoolean

  val modelType: RowType = SharedConf.denseModelType
  val numTask: Int = sharedConf.get(AngelConf.ANGEL_WORKERGROUP_NUMBER).toInt
  val mode = SharedConf.runningMode()

  val numSlot: Int = OptUtils.getSlotNum(optimizer)
  private val psRows: Int = numSlot + 1
  private val psCols = inputLayer.outputDim * outputDim
  private val weightCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
  private val biasCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_bias", 1, outputDim, modelType)
  graph.addMatrixCtx(weightCtx)
  graph.addMatrixCtx(biasCtx)

  lazy val weightId: Int = PSMatrixUtils.getMatrixId(s"${name}_weight")
  lazy val biasId: Int = PSMatrixUtils.getMatrixId(s"${name}_bias")

  @transient var forward: Matrix = _
  @transient var backward: Matrix = _
  @transient var output: Matrix = _
  @transient var gradOutput: Matrix = _
  @transient var ipOutputCache: Matrix = _

  @transient var weight: Matrix = _
  @transient var bias: Vector = _

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        inputLayer match {
          case ipLayer: Embedding => // from embedding layer
            ipLayer.calOutput() match {
              case mat: RBCompIntDoubleMatrix =>
                ipOutputCache = MatrixUtils.rbCompDense2Blas(mat)
                forward = ipOutputCache.dot(weight, parallel).add(bias)
              case mat: RBCompIntFloatMatrix =>
                ipOutputCache = MatrixUtils.rbCompDense2Blas(mat)
                forward = ipOutputCache.dot(weight, parallel).add(bias)
            }
          case ipLayer => // from other dense layer
            forward = ipLayer.calOutput().dot(weight, parallel).add(bias)
        }
        output = transFunc(forward)
        status = STATUS.Forward
      case _ =>
    }

    val end = System.currentTimeMillis()
    //    println(s"FCLayer($name) calOutput = ${end - start} ms")
    output
  }

  def calGradOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        //        println(s"the status in FCLayer($name)-calGradOutput is ${status.toString}")
        val gradTemp = gatherGrad()
        backward = transFunc.calGrad(output, gradTemp)

        gradOutput = inputLayer match {
          case ipLayer: Embedding =>
            modelType match {
              case RowType.T_DOUBLE_DENSE =>
                MatrixUtils.blas2RBCompDense(
                  Ufuncs.dot(backward, false, weight, true, parallel).asInstanceOf[BlasDoubleMatrix],
                  ipLayer.numFactors)
              case RowType.T_FLOAT_DENSE =>
                MatrixUtils.blas2RBCompDense(
                  Ufuncs.dot(backward, false, weight, true, parallel).asInstanceOf[BlasFloatMatrix],
                  ipLayer.numFactors
                )
            }
          case _ => Ufuncs.dot(backward, false, weight, true, parallel)
        }

        status = STATUS.Backward
      case _ =>
    }
    val end = System.currentTimeMillis()
    //    println(s"FCLayer($name) calGradOutput = ${end - start} ms")
    gradOutput
  }

  override def pullParams(epoch: Int): Unit = {
    weight = PSMatrixUtils.getRowAsMatrix(epoch, weightId, 0, inputLayer.outputDim, outputDim)
    bias = PSMatrixUtils.getRow(epoch, biasId, 0)
  }

  override def pushGradient(): Unit = {
    val normal = OptUtils.getNormal(mode, graph)
    status match {
      case STATUS.Backward =>
        val weightGrad: Matrix = if (ipOutputCache != null) {
          Ufuncs.dot(ipOutputCache, true, backward, false, parallel).idiv(normal)
        } else {
          Ufuncs.dot(inputLayer.calOutput(), true, backward, false, parallel).idiv(normal)
        }

        PSMatrixUtils.incrementRowByMatrix(weightId, numSlot, weightGrad)

        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.getLR / graph.taskNum))

        status = STATUS.Gradient
      case _ =>
    }
  }

  override def update(epoch: Int, batchSize: Int): Future[VoidResult] = {
    var result: Future[VoidResult] = null
    status match {
      case STATUS.Gradient =>
        result = optimizer.update(weightId, 1, epoch, batchSize)
        status = STATUS.Update
      case _ => throw new AngelException("STATUS Error, please calculate Gradient frist!")
    }
    result
  }

  override def init(taskflag: Int): Unit = {
    val bound: Double = 0.0001
    if (taskflag == 0) {
      val randFunc = new RandomNormal(weightId, 0, 0.0, bound)
      PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
    }
  }

  override def toString: String = {
    s"FCLayer name=${name} outputDim=$outputDim optimizer=$optimizer transFunc=${transFunc.getClass.getSimpleName}"
  }

  override def loadParams(loadContext: ModelLoadContext): Unit = {
    loadContext.addMatrix(new MatrixLoadContext(weightCtx.getName))
    loadContext.addMatrix(new MatrixLoadContext(biasCtx.getName))
  }

  override def saveParams(saveContext: ModelSaveContext): Unit = {
    val outputFormat = SharedConf.fcLayerMatrixOutputFormat
    val weightMCS: MatrixSaveContext = new MatrixSaveContext(weightCtx.getName, outputFormat)
    val biasMCS: MatrixSaveContext = new MatrixSaveContext(biasCtx.getName, outputFormat)
    weightMCS.addIndex(0)
    saveContext.addMatrix(weightMCS)
    saveContext.addMatrix(biasMCS)

  }

  override def toJson: JObject = {
    (ParamKeys.name -> name) ~
      (ParamKeys.typeName -> s"${this.getClass.getSimpleName}") ~
      (ParamKeys.outputDim -> outputDim) ~
      (ParamKeys.inputLayer, JString(inputLayer.name)) ~
      (ParamKeys.transFunc -> transFunc.toJson) ~
      (ParamKeys.optimizer -> optimizer.toJson)
  }
}
