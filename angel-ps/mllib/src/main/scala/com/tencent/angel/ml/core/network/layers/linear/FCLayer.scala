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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.layers.verge.Embedding
import com.tencent.angel.ml.core.network.transfunc.TransFunc
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.{MatrixUtils, RowType}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.layers.verge.Embedding
import com.tencent.angel.ml.core.network.transfunc.TransFunc
import com.tencent.angel.ml.core.network.variable.MatVariable.MatrixType
import com.tencent.angel.ml.core.network.variable.Variable
import com.tencent.angel.ml.core.network.variable.Variable.Location
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import org.apache.commons.logging.LogFactory


class FCLayer(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, override val optimizer: Optimizer
             )(implicit graph: Graph) extends LinearLayer(name, outputDim, inputLayer)(graph) with Trainable {
  val LOG = LogFactory.getLog(classOf[FCLayer])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf
  val parallel = sharedConf.get(MLConf.ML_MATRIX_DOT_USE_PARALLEL_EXECUTOR).toBoolean
  val modelType: RowType = SharedConf.denseModelType
  val numTask: Int = sharedConf.get(AngelConf.ANGEL_WORKERGROUP_NUMBER).toInt

  private val weight = Variable.getMatrix(s"${name}_weight", inputLayer.outputDim,
    outputDim, OptUtils.getSlotNum(optimizer), modelType, MatrixType.Blas, location)
  private val bias = Variable.getVector(s"${name}_bias", outputDim, modelType,
    location)

  @transient var forward: Matrix = _
  @transient var backward: Matrix = _
  @transient var output: Matrix = _
  @transient var gradOutput: Matrix = _
  @transient var ipOutputCache: Matrix = _

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
    weight.pullParams(epoch)
    bias.pullParams(epoch)
  }

  override def pushGradient(): Unit = {
    status match {
      case STATUS.Backward =>
        weight.pushGrads(inputLayer.calOutput(), backward)
        bias.pushGrads(backward, optimizer.getLR)
        status = STATUS.Gradient
      case _ =>
    }
  }

  override def update(epoch: Int, batchSize: Int): Future[VoidResult] = {
    var result: Future[VoidResult] = null
    status match {
      case STATUS.Gradient =>
        result = weight.update(optimizer, epoch, batchSize)
        bias.update(optimizer, epoch, batchSize)
        status = STATUS.Update
      case _ => throw new AngelException("STATUS Error, please calculate Gradient frist!")
    }
    result
  }

  override def init(taskFlag: Int): Unit = {
    weight.init(taskFlag, mean = 0.0, stddev = 0.000001)
    bias.init(taskFlag, mean = 0.0, stddev = 0.000001)
  }

  override def toString: String = {
    s"FCLayer name=$name outputDim=$outputDim optimizer=$optimizer transFunc=${transFunc.getClass.getSimpleName}"
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
