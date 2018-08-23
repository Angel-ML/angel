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


import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.{AngelConf, MatrixConf}
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.MatrixUtils
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.layers.edge.inputlayer.Embedding
import com.tencent.angel.ml.core.network.transfunc.TransFunc
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory


class FCLayer(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, override val optimizer: Optimizer
             )(implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) with Trainable {
  val LOG = LogFactory.getLog(classOf[FCLayer])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf

  val modelType: RowType = SharedConf.denseModelType
  val numTask: Int = sharedConf.get(AngelConf.ANGEL_WORKERGROUP_NUMBER).toInt

  val multiplier: Int = OptUtils.getOptMultiplier(optimizer)
  private val psRows: Int = multiplier
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
        //        println(s"the status in FCLayer($name)-calOutput is ${status.toString}")
        inputLayer match {
          case ipLayer: Embedding => // from embedding layer
            ipLayer.calOutput() match {
              case mat: RBCompIntDoubleMatrix =>
                ipOutputCache = MatrixUtils.rbCompDense2Blas(mat)
                forward = ipOutputCache.dot(weight).add(bias)
              case mat: RBCompIntFloatMatrix =>
                ipOutputCache = MatrixUtils.rbCompDense2Blas(mat)
                forward = ipOutputCache.dot(weight).add(bias)
            }
          case ipLayer => // from other dense layer
            forward = ipLayer.calOutput().dot(weight).add(bias)
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
                  Ufuncs.dot(backward, false, weight, true).asInstanceOf[BlasDoubleMatrix],
                  ipLayer.numFactors)
              case RowType.T_FLOAT_DENSE =>
                MatrixUtils.blas2RBCompDense(
                  Ufuncs.dot(backward, false, weight, true).asInstanceOf[BlasFloatMatrix],
                  ipLayer.numFactors
                )
            }
          case _ => Ufuncs.dot(backward, false, weight, true)
        }

        status = STATUS.Backward
      case _ =>
    }
    val end = System.currentTimeMillis()
    //    println(s"FCLayer($name) calGradOutput = ${end - start} ms")
    gradOutput
  }

  override def pullParams(): Unit = {
    weight = PSMatrixUtils.getRowAsMatrix(weightId, 0, inputLayer.outputDim, outputDim)
    bias = PSMatrixUtils.getRow(biasId, 0)
  }

  override def pushGradient(): Unit = {
    val normal = graph.placeHolder.getBatchSize * graph.taskNum
    status match {
      case STATUS.Backward =>
        val weightGrad: Matrix = if (ipOutputCache != null) {
          Ufuncs.dot(ipOutputCache, true, backward, false).idiv(normal)
        } else {
          Ufuncs.dot(inputLayer.calOutput(), true, backward, false).idiv(normal)
        }

        PSMatrixUtils.incrementRowByMatrix(weightId, multiplier - 1, weightGrad)

        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.lr / graph.taskNum))

        status = STATUS.Gradient
      case _ =>
    }
  }

  override def update(epoch: Int): Unit = {
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
    s"FCLayer name=${name} outputDim=$outputDim optimizer=$optimizer transFunc=${transFunc.getClass.getSimpleName}"
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
        weightMCS.addIndex(0)
        saveContext.addMatrix(weightMCS)
        saveContext.addMatrix(biasMCS)
      case _ =>
    }
  }
}
