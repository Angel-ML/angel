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

package com.tencent.angel.ml.matrixfactorization

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.DenseFloatVector
import com.tencent.angel.ml.matrixfactorization.threads._
import com.tencent.angel.ml.metric.ObjMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.worker.storage.{DataBlock, Reader}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.mutable.HashMap

/**
  * Learn a Matrix Factorization Model.
  *
  * @param ctx : the context of running task
  */

class MFLearner(override val ctx: TaskContext) extends MLLearner(ctx) {
  val LOG = LogFactory.getLog(classOf[MFLearner])

  // Matrix factorization model
  var mfModel = new MFModel(conf, ctx)

  // Number of threads that computes updation and validation
  val parallelism = 8
  // Parall task computes the updation with gradient descent method
  val pSgdTasks = new Array[PSgdTask](parallelism)
  // Parallel evaluate task
  val pEvaluateTasks = new Array[PEvaluateTask](parallelism)
  // Write user task
  val pWriteUserTask = new Array[PWriteUserTask](parallelism)
  // Thread executor pool
  val executor = new ThreadPoolExecutor(10, 10, 60, TimeUnit.MILLISECONDS, new
      LinkedBlockingQueue[Runnable])

  // Rank of user feature vector and item feature vector
  val rank: Int = conf.getInt(MLConf.ML_MF_RANK, MLConf.DEFAULT_ML_MF_RANK)
  // Number of all items, the row number of item matrix on PS
  val rRow: Int = conf.getInt(MLConf.ML_MF_ITEM_NUM, MLConf.DEFAULT_ML_MF_ITEM_NUM)
  if (rRow < 1)
    throw new AngelException("Item number should be set positive.")

  // Number of all tasks over all workers
  val totalTaskNum: Int = ctx.getTotalTaskNum
  // ID of current task
  val taskId: Int = ctx.getTaskId.getIndex
  // Pull item vecotrs from PS in #batchNum batches
  // pull #rowOneBatch item vectors(rows)in each batch
  val batchNum: Int = ctx.getConf.getInt(MLConf.ML_MF_ROW_BATCH_NUM, MLConf.DEFAULT_ML_MF_ROW_BATCH_NUM)
  val rowOneBatch: Int = rRow / batchNum

  // Algorithm parameters
  // Learning rate for gradient descent
  val eta: Double = conf.getDouble(MLConf.ML_MF_ETA, MLConf.DEFAULT_ML_MF_ETA)
  // Regularization parameter
  val lambda: Double = conf.getDouble(MLConf.ML_MF_LAMBDA, MLConf.DEFAULT_ML_MF_LAMBDA)
  // Iteration number
  val epochNum = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)

  //val logger = DistributedLogger(ctx)
  //logger.setNames("localLoss", "globalLoss")


  /**
    * Train a matrix factorizaiton model
    *
    * @param trainData   : trainning dataset storage
    * @param validataSet : validate dataset storage
    * @return : a learned model
    */
  override
  def train(trainData: DataBlock[LabeledData], validataSet: DataBlock[LabeledData]): MLModel = {
    LOG.info(s"Start to train Matrix Factorizaiton Model. #Item=$rRow, #rank=$rank, #rowBatch=$batchNum")

    init()
    LOG.info("mfModel.userNum=" + mfModel.userNum)
    globalMetrics.addMetric(MFModel.MF_METRIC, ObjMetric())

    while (ctx.getEpoch < epochNum) {
      LOG.info(s"Start Epoch ${ctx.getEpoch}")
      oneEpoch(ctx.getEpoch)

      ctx.incEpoch()
    }

    writeUserVectors

    //logger.close()
    mfModel
  }

  /**
    * Parse a text line into a UserVec instance
    *
    * @param value : a line of text
    * @return : a UserVec instance
    */
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
      val itemId = splits(i * 2 + 1).toInt
      val rating = splits(i * 2 + 2).toInt
      itemIds(i) = itemId
      ratings(i) = rating
    }

    new UserVec(userId, itemIds, ratings)
  }


  /**
    * Parse input text into trainning data
    */
  @throws[IOException]
  @throws[InterruptedException]
  @throws[ClassNotFoundException]
  def readUsers(): HashMap[Int, UserVec] = {
    val start = System.currentTimeMillis()

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

    val time = System.currentTimeMillis() - start
    LOG.info(s"Task[${ctx.getTaskId.getIndex}] read ${users.size} users success, cost $time ms")

    users
  }

  /**
    * Initialize item matrix in #batchNum batches, initialize #rowOneBath rows in each batch
    */
  def initItemsMat() {
    LOG.info(s"Task[${ctx.getTaskId.getIndex}] Init vector matrix. #totalTaskNum=$totalTaskNum " +
      s"#batch=$batchNum #rowsOneBatch=" + rowOneBatch)

    for (i <- 0 until batchNum) {
      val low = i * rowOneBatch
      var up = (i + 1) * rowOneBatch
      if (up + rowOneBatch > rRow)
        up = rRow

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
    // Only build user vectors for users appear in train dataset for current task
    mfModel.buildUsers(readUsers())

    // Only build item vectors for items appear in train dataset for current task
    Utils.buildItems(mfModel)

    // Init item vectors on PS with random values, to support larger model, we initiallize item
    // vectors in #batchNum batches, each batch only initiallize #rowOneBatch rows
    initItemsMat()

    // Global barrier, waiting for all workers init finished
    ctx.globalSync(mfModel.itemMat.getMatrixId)

    LOG.info("All workers init model success.")

    // Run sgd computing in #parallelism tasks
    for (i <- 0 until parallelism) {
      pSgdTasks(i) = new PSgdTask(mfModel.users, mfModel.items, ctx, eta, lambda, rank)
    }

    // Run evaluating in #parallelism tasks
    for (i <- 0 until parallelism) {
      pEvaluateTasks(i) = new PEvaluateTask(mfModel.users, mfModel.items, eta, lambda, rank)
    }
  }

  def oneEpoch(epoch: Int) {
    val startIter = System.currentTimeMillis
    val startTrain = System.currentTimeMillis

    trainOneEpoch(epoch)

    val trainTime = System.currentTimeMillis - startTrain
    val startVali = System.currentTimeMillis

    val loss = validateOneEpoch(epoch)

    val validTime = System.currentTimeMillis - startVali
    val iterTime = System.currentTimeMillis - startIter

    globalMetrics.metric(MFModel.MF_METRIC, loss)
    val infoMsg = s"Task[${ctx.getTaskId.getIndex}] Epoch=$epoch success. local loss=$loss, " +
      s" epoch cost $iterTime ms, train cost $trainTime ms, validate cost $validTime ms"
    LOG.info(infoMsg)
  }

  /**
    * Train mf model in one epoch
    *
    * @param epoch : epoch ID
    */
  def trainOneEpoch(epoch: Int) {
    val rowOneBatch = rRow / batchNum

    for (batch <- 0 until batchNum) {
      val start = System.currentTimeMillis()

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

      val batchTime = System.currentTimeMillis() - start
      LOG.debug(s"Task[${ctx.getTaskId.getIndex}] Epoch[$epoch] batch[$batch] cost $batchTime ms")
    }
  }


  /**
    * Evaluate loss values in one epoch
    *
    * @param epoch : epoch ID
    * @return : loss values of local users and realated items
    */
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

  /**
    * Calculate user vectors regularized loss value
    *
    * @return : user vectors regularized loss value
    */
  def getLRReg: Double = {
    var loss = 0.0

    for (userVec <- mfModel.users.values) {
      loss += Utils.lossOneRow(userVec.getFeatures, lambda)
    }

    loss
  }

  def writeUserVectors: Unit = {
    var idx = new AtomicInteger(0);

    val future = new Array[Future[Boolean]](parallelism)
    for (i <- 0 until parallelism) {
      pWriteUserTask(i) = new PWriteUserTask(ctx, mfModel.users, idx, i)
      future(i) = executor.submit(pWriteUserTask(i));
    }

    for (i <- 0 until parallelism) {
      future(i).get()
    }
  }
}
