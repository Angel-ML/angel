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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.matrixfactorization

import java.io.IOException
import java.util.concurrent.{Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.MLLearner
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.matrixfactorization.utils.{PEvaluateTask, PSgdTask, UserVec, Utils}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.DenseFloatVector
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.worker.storage.{Reader, Storage}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.mutable.HashMap


class MFLearner(override val ctx: TaskContext) extends MLLearner(ctx){
  val LOG = LogFactory.getLog(classOf[MFLearner])

  var mfModel = new MFModel(ctx, conf)

  val parallelism = 8
  val pSgdTasks = new Array[PSgdTask](parallelism)
  val pEvaluateTasks = new Array[PEvaluateTask](parallelism)
  val executor = new ThreadPoolExecutor(10, 10, 60, TimeUnit.MILLISECONDS, new
      LinkedBlockingQueue[Runnable])

  val rank: Int = conf.getInt(MLConf.ML_MF_RANK, 200)
  val rRow: Int = conf.getInt(MLConf.ML_MF_ITEM_NUM, -1)

  val totalTaskNum: Int = ctx.getTotalTaskNum
  val taskId: Int = ctx.getTaskId.getIndex
  val batchNum: Int = ctx.getConf.getInt(MLConf.ML_MF_ROW_BATCH_NUM, 1)
  val rowOneBatch: Int = rRow / batchNum

  // Algorithm parameters
  val eta: Double = conf.getDouble(MLConf.ML_MF_ETA, 0.0054)
  val lambda: Double = conf.getDouble(MLConf.ML_MF_LAMBDA, 0.01)

  override def train(dataSet: Storage[LabeledData]): Unit = {
    init()

    val epochNum = conf.getInt(MLConf.ML_EPOCH_NUM, 20)

    //Train user vectors and item vectors iteratively.
    while (ctx.getIteration <  epochNum) {
      //mf.oneEpoch(ctx.getIteration)
      oneEpoch(ctx.getIteration)
      ctx.increaseIteration()
    }

  }

  def parseLine(value: Text): UserVec = {
    val str = value.toString
    val splits = str.split(" ")
    if (splits.length < 1) {
      return null
    }
    val userId = splits(0).toInt
    val length = splits.length
    val numRatings = (length - 1) / 2
    val itemIds = new Array[Int](numRatings)
    val ratings = new Array[Int](numRatings)

    for (i <- 0 until numRatings) {
      val itemId: Int = splits(i * 2 + 1).toInt
      val rating: Int = splits(i * 2 + 2).toInt
      itemIds(i) = itemId
      ratings(i) = rating
    }

    new UserVec(userId, itemIds, ratings)
  }

  @throws[IOException]
  @throws[InterruptedException]
  @throws[ClassNotFoundException]
  def readUsers(): HashMap[Int, UserVec] = {
    val users = new HashMap[Int, UserVec]

    val reader = ctx.getReader.asInstanceOf[Reader[LongWritable, Text]]
    while (reader.nextKeyValue) {
      val text = reader.getCurrentValue
      val user = parseLine(text)
      if (user != null) {
        user.setRank(rank)
        user.initFeatures()
        users.put(user.getUserId, user)
      }
    }
    LOG.info("Read user line " + users.size)

    users
  }

    def initItemsMat() {
      LOG.info("initRmat totalTaskNum=" + totalTaskNum + " taskId=" + taskId + " batchNum=" +
        batchNum + " rowOneBatch=" + rowOneBatch)

      for (i <- 0 until batchNum) {
        val low = i * rowOneBatch
        var up = (i + 1) * rowOneBatch
        if (up + rowOneBatch > rRow)
          up = rRow

        LOG.info("initRmat batch=" + i + " low= " + low + " up=" + up);

        for (j <- low until up) {
          if (j % totalTaskNum == taskId) {
            val update = new DenseFloatVector(rank)

            for (col <- 0 until rank) {
              update.set(col, Math.random().toFloat)
            }

            mfModel.itemMat.increment(j, update)
          }
        }

        val f = mfModel.itemMat.clock()
        f.get()
      }
    }

  def init(): Unit = {
    // Load user vectors
    mfModel.buildUsers(readUsers())

    // Build item vectors
    Utils.buildItems(mfModel)

    // Init item m
    initItemsMat()

    // Global barrier, waiting for all workers init finished
    ctx.globalSync(mfModel.itemMat.getMatrixId)
    LOG.info("All workers init model success.")


    for (i <- 0 until parallelism) {
      pSgdTasks(i) = new PSgdTask(mfModel.users, mfModel.items, ctx, eta, lambda, rank)
    }

    for (i <- 0 until parallelism) {
      pEvaluateTasks(i) = new PEvaluateTask(mfModel.users, mfModel.items, eta, lambda, rank)
    }
  }

  def oneEpoch(epoch: Int) {
    LOG.info("epoch=" + epoch)

    val startIter = System.currentTimeMillis
    val startTrain = System.currentTimeMillis

    trainOneEpoch(epoch)

    val trainTime = System.currentTimeMillis - startTrain
    val startTest = System.currentTimeMillis

    val loss = validateOneEpoch(epoch)

    val testTime = System.currentTimeMillis - startTest
    val iterTime = System.currentTimeMillis - startIter

    val infoMsg = "IterInfo Epoch=" + epoch + " LossHelper=" + loss + " iterTime=" +
      iterTime + " trainTime=" + trainTime + " testTime=" + testTime
    LOG.info(infoMsg)
    if (conf.get(AngelConfiguration.ANGEL_DEPLOY_MODE) eq "LOCAL") conf.set("loss", "" + loss)

  }

  def trainOneEpoch(epoch: Int) {
    val rowOneBatch = rRow / batchNum

    for (batch <- 0 until batchNum) {
      val rowIndex = new RowIndex()
      val low = batch * rowOneBatch
      var up = (batch + 1) * rowOneBatch
      if (up + rowOneBatch > rRow)
        up = rRow

      for (i <- low until up) {
        if (mfModel.usedItemIDs(i) == 1) {
          rowIndex.addRowId(i)
        }
      }

      // Get used items vectors from PS.
      val rows = mfModel.itemMat.getRowsFlow(rowIndex, 100)
      // update user vectors and item vectors with SGD.
      val future = new Array[Future[Boolean]](parallelism)
      for (i <- 0 until parallelism) {
        pSgdTasks(i).setTaskQueue(rows)
        future(i) = executor.submit(pSgdTasks(i))
      }

      for (i <- 0 until parallelism) {
        try {
          future(i).get()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }

      val f = mfModel.itemMat.clock()
      f.get()
    }
  }


  @throws[InterruptedException]
  def validateOneEpoch(epoch: Int): Double = {

    val rowOneBatch = rRow / batchNum

    var totalLoss = 0.0

    for (batch <- 0 until batchNum) {
      val rowIndex = new RowIndex()

      val low = batch * rowOneBatch
      var up = (batch + 1) * rowOneBatch
      if (up + rowOneBatch > rRow)
        up = rRow

      LOG.info("validate low = " + low + " up=" + up)

      for (i <- low until up) {
        if (mfModel.usedItemIDs(i) == 1) {
          rowIndex.addRowId(i)
        }
      }


      val rows = mfModel.itemMat.getRowsFlow(rowIndex, 100)

      val future = new Array[Future[Double]](parallelism)
      for (i <- 0 until parallelism) {
        pEvaluateTasks(i).setTaskQueue(rows)
        future(i) = executor.submit(pEvaluateTasks(i))
      }

      for (i <- 0 until parallelism) {
        try {
          totalLoss += future(i).get()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }

    totalLoss += getLRReg
    totalLoss

  }

  def getLRReg: Double = {
    var loss = 0.0

    for (userVec <- mfModel.users.values) {
      loss += Utils.lossOneRow(userVec.getFeatures, lambda)
    }
    loss
  }


}
