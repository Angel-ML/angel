/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.optimizer2

import java.util

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TUpdate
import com.tencent.angel.ml.math.matrix._
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.optimizer2.utils.OptUtils
import com.tencent.angel.worker.storage.DataBlock
import org.apache.commons.logging.LogFactory

import scala.math.Numeric
import scala.reflect.runtime.universe._

// https://zhuanlan.zhihu.com/p/22252270
// http://www.jmlr.org/papers/volume12/duchi11a/duchi11a.pdf
// http://ruder.io/optimizing-gradient-descent/

abstract class Optimizer(var batchSize: Int, val numUpdatePerEpoch: Int, var lr: Double) {
  private val LOG = LogFactory.getLog(classOf[Optimizer])
  protected var globalParams: util.HashMap[String, TUpdate] = _
  protected var localParams: util.HashMap[String, TUpdate] = _
  protected var grad: util.HashMap[String, TUpdate] = _
  var epoch: Int = 0

  def optimize[N: Numeric : TypeTag](trainData: DataBlock[LabeledData], model: OptModel, indexes: Array[N]): (util.HashMap[String, TUpdate], Double) = {
    var (pull, calulate, push) = (0l, 0l, 0l)
    var (startpull, stoppull) = (0l, 0l)
    var (startcalulate, stopcalulate) = (0l, 0l)
    var (pushstart, pushstop) = (0l, 0l)

    startpull = System.currentTimeMillis()
    // 1. pull parameters from PS as globalParams
    globalParams = model.pullParamsFromPS(indexes, model.getIndexFlag)
    stoppull = System.currentTimeMillis()
    pull += stoppull - startpull

    startcalulate = System.currentTimeMillis()
    // 2. initial ...
    // 2.1 initial gradient
    if (grad == null) {
      grad = model.getZeroParams
    } else {
      OptUtils.clear(grad)
    }

    // 2.2 initial other parameters
    initialLocal(model, localParams, globalParams)

    // 2.3 initial localParams by clone globalParams
    localParams = OptUtils.clone(globalParams)

    // 3. training (Note: just train one ecpch)
    var (loss, lastLogFlag) = (0.0, -1)
    var (inbatchCount, batchCount, pullpushFlag) = (0, 0, false)

    if ((trainData.size + numUpdatePerEpoch - 1) / numUpdatePerEpoch < batchSize) {
      batchSize = (trainData.size + numUpdatePerEpoch - 1) / numUpdatePerEpoch
    }
    val totalBatch = (trainData.size + batchSize - 1) / batchSize
    val numBatchPerUpdate = (totalBatch + numUpdatePerEpoch - 1) / numUpdatePerEpoch
    for (idx <- 0 until trainData.size) {
      val data = trainData.loopingRead()
      // note: loopingRead
      val (x, y) = (data.getX, data.getY)

      // calculate the loss and grad of a sample, and update gradient
      // Note: a) we do not update localParams here
      //       b) only update the grad of empirical risk, ignore the gradient of regularization
      loss += model.calLossAndUpdateGrad(x, y, localParams, grad)

      inbatchCount += 1

      // this pice of core is used to print logs, just skip!
      val thisLogFlag = idx / (trainData.size / 10)
      if (thisLogFlag != lastLogFlag && thisLogFlag != 0) {
        LOG.info(s"Process: ${thisLogFlag}0% of samples have finished ! The current average loss is ${loss / (idx + 1)}")
        lastLogFlag = thisLogFlag
      }

      // for each minibatch, we update the local parameters and clear gradient
      // Note: we do not push parameters at all here, because push for each minibatch is
      //       not efficient in some condition
      if (inbatchCount == batchSize || (idx + 1) == trainData.size) {
        batchCount += 1
        pullpushFlag = true
        // a). update local parameters
        val numSampesInBath = if ((idx + 1) == trainData.size) trainData.size % batchSize else batchSize
        updateLocal(model, numSampesInBath, epoch * totalBatch + batchCount)

        // b). clear gradinet to take on the next batch
        OptUtils.clear(grad)
        inbatchCount = 0
      }

      // push and pull parameter
      if (pullpushFlag && (batchCount % numBatchPerUpdate == 0 || (idx + 1) == trainData.size)) {
        // a) calcute delta since last push
        val delta = OptUtils.axpy(localParams, globalParams, -1.0)
        stopcalulate = System.currentTimeMillis()
        calulate += stopcalulate - startcalulate

        // b) push delta, now localParams is the delta
        pushstart = System.currentTimeMillis()
        model.pushParamsToPS(delta)
        pushstop = System.currentTimeMillis()
        push += pushstop - pushstart

        // c) send psf to PS, calculate on PS
        startcalulate = System.currentTimeMillis()
        psfHook(model, batchSize)
        stopcalulate = System.currentTimeMillis()
        calulate += stopcalulate - startcalulate

        // d) pull new parameters form PS
        startpull = System.currentTimeMillis()
        globalParams = model.pullParamsFromPS(indexes, model.getIndexFlag)
        stoppull = System.currentTimeMillis()
        pull += stoppull - startpull

        startcalulate = System.currentTimeMillis()
        // e) clear gradient and monment, because a new generation begins
        //    we are not necessary clear grad here, since it has cleared
        initialLocal(model, localParams, globalParams)

        // f) clone the newly pushed parameter as local parameters
        //    and from now on, we only operate local parameters
        localParams = OptUtils.clone(globalParams)

        pullpushFlag = false
      }
    }

    val sparsity = model.calSparsity(globalParams)
    LOG.info(s"The nonZeroNumber ratio is $sparsity")

    LOG.info(s"The pull, calculate, push time is this epoch are $pull, $calulate and $push respectively !")
    (localParams, loss)
  }

  protected def updateLocal(model: OptModel, numSample: Int, iterCount: Int): Unit

  protected def initialLocal(model: OptModel, local: util.HashMap[String, TUpdate], global: util.HashMap[String, TUpdate]): Unit

  protected def psfHook(model: OptModel, numSample: Int): Unit = {}

}
