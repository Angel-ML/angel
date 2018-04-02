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

package com.tencent.angel.example.ml

import java.io.{BufferedReader, InputStreamReader}
import java.util
import java.util.Collections
import java.util.concurrent.Future

import com.tencent.angel.PartitionKey
import com.tencent.angel.conf.AngelConf
import AngelConf._
import com.tencent.angel.ml.MLRunner
import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{TDoubleVector, DenseDoubleVector, DenseIntVector}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.impl.matrix.{ServerDenseDoubleRow, ServerRow}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.mutable.ArrayBuffer

class ReduceRunner extends MLRunner {
  /**
    * Training job to obtain a model
    */
  override def train(conf: Configuration): Unit = {
    train(conf, new ReduceModel(conf), classOf[ReduceTask])
  }

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = ???

  /**
    * Using a model to predict with unobserved samples
    */
  override def predict(conf: Configuration): Unit = ???
}


class ReduceTask(ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {

  val feaNum = ctx.getConf.getInt(ML_FEATURE_INDEX_RANGE, 100)

  val LOG = LogFactory.getLog(classOf[ReduceTask])

  val model = new ReduceModel(ctx.getConf, ctx)

  override
  def train(ctx: TaskContext) = {

    dstat()

    model.model.clock(false)
    ctx.incEpoch()


    val data = new Array[Double](feaNum)
    for (i <- 0 until feaNum)
      data(i) = i

    val update = new DenseDoubleVector(feaNum, data)

    //    val startUpdate = System.currentTimeMillis

    model.model.increment(0, update)

    Thread.sleep(5000)

    LOG.info(s"waiting for others")
    ctx.globalSync()
    LOG.info(s"barriers get")




    val startUpdate = System.currentTimeMillis

    model.model.syncClock()

    val startGet = System.currentTimeMillis

//    val row = model.model.getRow(0)

    pipelineGet()

    val finishGet = System.currentTimeMillis

    LOG.info(s"updateCost=${startGet - startUpdate} getCost=${finishGet - startGet} " +
      s"totalCost=${finishGet - startUpdate}")

    val taskId = ctx.getTaskIndex
    var timeUpdate = new DenseIntVector(model.workerNum)
    timeUpdate.set(taskId, (startGet - startUpdate).toInt)
    model.time.increment(0, timeUpdate)
    timeUpdate = new DenseIntVector(model.workerNum)
    timeUpdate.set(taskId, (finishGet - startGet).toInt)
    model.time.increment(1, timeUpdate)
    model.time.clock().get()

    if (taskId == 0) {
      val updateCost = model.time.getRow(0).asInstanceOf[TDoubleVector]
      val getCost = model.time.getRow(1).asInstanceOf[TDoubleVector]
      for (i <- 0 until model.workerNum) {
        LOG.info(s"task[$i] update=${updateCost.get(i)} get=${getCost.get(i)} " +
          s"total=${updateCost.get(i) + getCost.get(i)}")
      }
    }
  }


  def dstat() = {
    val p = Runtime.getRuntime.exec("dstat")
    val input = new BufferedReader(new InputStreamReader(p.getInputStream))
    var finish = false
    while (!finish) {
      input.readLine() match {
        case null => finish = true
        case s: String => LOG.info(s)
      }
    }
  }


  def manuallyGet(): Unit = {

    val keys = PSAgentContext.get().getMatrixMetaManager.getPartitions(model.model.getMatrixId)
    val client = PSAgentContext.get().getMatrixTransportClient
    val futures = new ArrayBuffer[Future[ServerRow]]
    val splits = new ArrayBuffer[ServerRow]()

    Collections.shuffle(keys)
    for (i <- 0 until keys.size()) {
      val key = keys.get(i)
      val f = client.getRowSplit(key, 0, 0)
      futures.append(f)
    }

    var firstSplit: ServerRow = null
    for (elem <- futures) {
      val row = elem.get()
      splits.append(row)
      if (row.getStartCol == 0)
        firstSplit = row
    }

    val row = firstSplit

    LOG.info(s"rowId[${row.getRowId}] startCol=[${row.getStartCol}]")

    row.getRowType match {
      case RowType.T_DOUBLE_DENSE =>
        val d = row.asInstanceOf[ServerDenseDoubleRow]
        LOG.info(s"second value = [${d.getData.get(1)}]")
      case _ =>

    }
  }

  def pipelineGet() = {
    var keys = PSAgentContext.get().getMatrixMetaManager.getPartitions(model.model.getMatrixId)
    val client = PSAgentContext.get().getMatrixTransportClient
    val futures = new ArrayBuffer[Future[ServerRow]]
    val splits = new ArrayBuffer[ServerRow]()

    val cache = PSAgentContext.get().getClockCache.getMatrixClockCache(model.model.getMatrixId())

    Collections.shuffle(keys)

    val clock = ctx.getMatrixClock(model.model.getMatrixId())

    var finish = false
    while (!finish) {
      val left = new util.ArrayList[PartitionKey]()
      val iter = keys.iterator()
      while (iter.hasNext) {
        val key = iter.next()
        if (cache.getClock(key) >= clock) {
          val f = client.getRowSplit(key, 0, clock)
          futures.append(f)
        } else {
          left.add(key)
        }
      }

      if (left.size() == 0) {
        finish = true
      } else {
        keys = left
      }

      LOG.info(s"left parts size=${keys.size()}")

      Thread.sleep(1000)
    }

    var firstSplit: ServerRow = null
    for (elem <- futures) {
      val row = elem.get()
      splits.append(row)
      if (row.getStartCol == 0)
        firstSplit = row
    }

    val row = firstSplit

    LOG.info(s"rowId[${row.getRowId}] startCol=[${row.getStartCol}]")

    row.getRowType match {
      case RowType.T_DOUBLE_DENSE =>
        val d = row.asInstanceOf[ServerDenseDoubleRow]
        LOG.info(s"second value = [${d.getData.get(1)}]")
      case _ =>

    }

  }

  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

  override
  def preProcess(ctx: TaskContext): Unit = {

  }

}

class ReduceModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {

  val name = "model"

  val feaNum = conf.getInt(ML_FEATURE_INDEX_RANGE, 100)
  val workerNum = conf.getInt(ANGEL_WORKERGROUP_NUMBER, 10)

  val ps = conf.getInt(ANGEL_PS_NUMBER, 1)
  val part = conf.getInt(ML_MODEL_PART_PER_SERVER, 1)

  val model = PSModel(name, 1, feaNum, 1, feaNum / ps / part).setRowType(RowType.T_DOUBLE_DENSE)
  val time  = PSModel("time", 2, workerNum).setRowType(RowType.T_INT_DENSE).setOplogType("DENSE_INT")

  addPSModel(name, model)
  addPSModel("time", time)

  setSavePath(conf)
  setLoadPath(conf)


  /**
    * Predict use the PSModels and predict data
    *
    * @param storage predict data
    * @return predict result
    */
  override def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = ???

}
