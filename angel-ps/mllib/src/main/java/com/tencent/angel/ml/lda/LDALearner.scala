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

package com.tencent.angel.ml.lda

import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.{Collections, Random}

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.lda.structures.S2STraverseMap
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, DenseIntVector, SparseIntSortedVector}
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.utils.MathUtils
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.commons.logging.LogFactory
import org.apache.commons.math.special.Gamma


class LDALearner(ctx: TaskContext,
                 docs: util.List[Document],
                 lDAModel: LDAModel) extends MLLearner(ctx) {

  val LOG = LogFactory.getLog(classOf[LDALearner])

  val ck  = new AtomicIntegerArray(lDAModel.K)
  val cdk = new Array[S2STraverseMap](docs.size())

  val words = buildWords()
  val rowIds = new Array[Int](words.size())

  def buildWords(): util.Map[Integer, TokensOneWord] = {
    val builder = new util.HashMap[Int, IntArrayList]()
    val M = docs.size()
    for (idx <- 0 until M) {
      val doc = docs.get(idx)
      val size = doc.len()
      for (i <- 0 until size) {
        val wid = doc.wids(i)
        if (!builder.containsKey(wid))
          builder.put(wid, new IntArrayList())
        builder.get(wid).add(doc.docId)
      }
    }

    val words = new util.HashMap[Integer, TokensOneWord]()
    val iter = builder.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      words.put(entry.getKey, new TokensOneWord(entry.getKey, entry.getValue))
    }

    words
  }

  val localWords = new Array[Boolean](lDAModel.V)

  val executor = new ThreadPoolExecutor(lDAModel.threadNum, lDAModel.threadNum * 2,
    1, TimeUnit.HOURS, new LinkedBlockingQueue[Runnable]())
  val threads = new Array[SamplingThread](lDAModel.threadNum)
  for (i <- 0 until lDAModel.threadNum)
    threads(i) = new SamplingThread(lDAModel, ck, words, cdk, docs)


  @throws[Exception]
  def initOneBatch(queue: ConcurrentLinkedQueue[TokensOneWord], batchNum: Int): Unit = {
    LOG.info(s"start init batch $batchNum")

    for (i <- 0 until lDAModel.K)
      ck.set(i, 0)

    val initThreads = new Array[InitThread](lDAModel.threadNum)
    val futures = new Array[Future[java.lang.Boolean]](lDAModel.threadNum)

    for (i <- 0 until lDAModel.threadNum) {
      initThreads(i) = new InitThread(queue)
      futures(i) = executor.submit(initThreads(i))
    }

    waitForFutures(futures)

    val update = new DenseIntVector(lDAModel.K)
    update.setRowId(0)

    for (i <- 0 until ck.length())
      update.set(i, ck.get(i))


    lDAModel.tMat.increment(update)
    val f1 = lDAModel.tMat.clock()
    val f2 = lDAModel.wtMat.clock()
    f1.get()
    f2.get()
  }

  def buildInitQueue(splitNum: Int): util.List[ConcurrentLinkedQueue[TokensOneWord]] = {
    val len = rowIds.length
    val splitRowNum = len / splitNum + 1
    val result = new util.ArrayList[ConcurrentLinkedQueue[TokensOneWord]]()
    var i = 0
    while (i < len) {
      val queue = new ConcurrentLinkedQueue[TokensOneWord]()
      for (j <- i until Math.min(i + splitRowNum, len))
        queue.add(words.get(rowIds(j)))
      i += splitRowNum
      result.add(queue)
    }
    result
  }

  @throws[Exception]
  def init(): Unit = {
    LOG.info("Start init")
    val iter = words.entrySet().iterator()

    var idx = 0
    while (iter.hasNext) {
      val list = iter.next().getValue
      rowIds(idx) = list.getWordId
      localWords(list.getWordId) = true
      idx += 1
    }

    MathUtils.shuffle(rowIds)

    val queues = buildInitQueue(lDAModel.splitNum)
    for (split <- 0 until queues.size())
      initOneBatch(queues.get(split), split)
  }

  def buildRowIndex(splitNum: Int): util.List[RowIndex] = {
    val len = rowIds.length
    val splitRowNum = len / splitNum + 1
    val indexList = new util.ArrayList[RowIndex]()
    var i = 0
    while (i < len) {
      val index = new RowIndex()
      for (j <- i until Math.min(i + splitRowNum, len))
        index.addRowId(rowIds(j))
      indexList.add(index)
      i += splitRowNum
    }
    indexList
  }

  def getCk(): Unit = {
    var ckSum = 0
    // Get topic mat
    lDAModel.tMat.getRow(0) match {
      case x: DenseIntVector =>
        for (i <- 0 until x.size()) {
          ck.set(i, x.get(i))
          ckSum += x.get(i)
        }

      case y => throw new AngelException("Should be DenseIntVector while it is " + y.getClass.getName)
    }

    LOG.info(s"ck_sum=$ckSum")
  }

  @throws[Exception]
  def trainOneBatch(batchNum: Int, rowIndex: RowIndex): Unit = {
    LOG.info("Start batch " + batchNum + " #rows = " + rowIndex.getRowsNumber)

    getCk()

    val rows = lDAModel.wtMat.getRowsFlow(rowIndex, 1000)
    val futures = new Array[Future[java.lang.Boolean]](lDAModel.threadNum)
    for (i <- 0 until lDAModel.threadNum) {
      threads(i).setTaskQueue(rows)
      futures(i) = executor.submit(threads(i))
    }

    waitForFutures(futures)

    val f1 = lDAModel.tMat.clock()
    val f2 = lDAModel.wtMat.clock()
    f1.get()
    f2.get()

    ctx.incIteration()
  }

  @throws[Exception]
  def trainOneEpoch(epoch: Int): Unit = {
    LOG.info(s"start epoch $epoch")
    val indexList = buildRowIndex(lDAModel.splitNum)
    Collections.shuffle(indexList)

    for (idx <- 0 until indexList.size())
      trainOneBatch(idx, indexList.get(idx))

    if (epoch % 5 == 0) {
      val llh = loglikelihood()
      val update = new DenseDoubleVector(lDAModel.epoch + 1);
      update.set(epoch, llh)
      lDAModel.llh.increment(0, update)
      lDAModel.llh.clock().get()
      val globalLLh = lDAModel.llh.getRow(0).get(epoch)
      LOG.info(s"epoch=$epoch localllh=$llh globalllh=$globalLLh")
    }
  }

  @throws[Exception]
  def loglikelihood(): Double = {
    val result = computeDocllh()
    val taskId = ctx.getTaskId.getIndex
    val taskNum = ctx.getTotalTaskNum

    val numPerTask = Math.ceil(lDAModel.V * 1.0 / taskNum).toInt + 1
    val start = numPerTask * taskId
    val end   = Math.min(lDAModel.V, numPerTask * (taskId + 1))
    result.llh += computeWordllh(start, end)
    if (taskId == 0)
      result.llh += computeWordSummaryllh

    result.llh
  }

  def computeDocllh(): LLhwResult = {
    val result = new LLhwResult

    var tokenNum = 0
    var dkNum = 0
    val iter = docs.iterator()
    var ll = 0.0
    while (iter.hasNext) {
      val doc = iter.next()
      val dk = cdk(doc.docId)
      if (dk != null) {
        tokenNum += doc.len()
        dkNum += dk.size
        for (j <- 0 until dk.size)
          ll += Gamma.logGamma(lDAModel.alpha + dk.value(dk.idx(j)))
        ll -= Gamma.logGamma(doc.len + lDAModel.alpha * lDAModel.K)
      }
    }

    ll -= dkNum * Gamma.logGamma(lDAModel.alpha)
    ll += docs.size() * Gamma.logGamma(lDAModel.alpha * lDAModel.K)
    result.llh = ll;
    result.tokenNum = tokenNum
    result
  }

  @throws[Exception]
  def computeWordllh(start: Int, end: Int): Double = {
    LOG.info(s"compute word llh start=$start end=$end")
    val rowIndex = new RowIndex()

    for (w <- start until end) {
      rowIndex.addRowId(w)
    }

    val rows = lDAModel.wtMat.getRowsFlow(rowIndex, 1000)

    var ll = 0.0
    val lgammaBeta = Gamma.logGamma(lDAModel.beta)

    var finish = false;
    while (!finish) {
      rows.take() match {
        case row: DenseIntVector => ll += computeWordllh(row, lgammaBeta)
        case row: SparseIntSortedVector => ll += computeWordllh(row, lgammaBeta)
        case null => finish = true
      }
    }

    ll
  }

  def computeWordllh(vector: DenseIntVector, lgammaBeta: Double): Double = {
    var ll = 0.0
    val vals = vector.getValues
    for (k <- 0 until vals.length)
      if (vals(k) > 0)
        ll += Gamma.logGamma(vals(k) + lDAModel.beta) - lgammaBeta
    ll
  }

  def computeWordllh(vector: SparseIntSortedVector, lgammaBeta: Double): Double = {
    var ll = 0.0
    val vals = vector.getValues
    for (i <- 0 until vals.length)
      if (vals(i) > 0)
        ll += Gamma.logGamma(vals(i) + lDAModel.beta) - lgammaBeta
    ll
  }

  def computeWordSummaryllh(): Double = {
    getCk()
    LOG.info("compute summary llh")
    var ll = 0.0
    ll += lDAModel.K * Gamma.logGamma(lDAModel.beta * lDAModel.V)
    for (k <- 0 until lDAModel.K)
      ll -= Gamma.logGamma(ck.get(k) + lDAModel.beta * lDAModel.V)
    ll
  }

  @throws[Exception]
  def waitForFutures(futures: Array[Future[java.lang.Boolean]]): Unit = {
    futures.foreach(f => f.get())
  }


  class InitThread(queue: ConcurrentLinkedQueue[TokensOneWord]) extends Callable[java.lang.Boolean] {

    @throws[Exception]
    override
    def call(): java.lang.Boolean = {
      val rand = new Random(System.currentTimeMillis())

      var finish = false
      while (!finish) {
        queue.poll() match {
          case list: TokensOneWord =>
            val wid = list.getWordId
            val update = new DenseIntVector(lDAModel.K)
            update.setRowId(wid)

            val len = list.size()

            for (j <- 0 until len) {
              val did = list.getDocId(j)
              val topic = rand.nextInt(lDAModel.K)
              val doc = docs.get(did)
              list.setTopic(j, topic)
              update.inc(topic, 1)
              ck.addAndGet(topic, 1)

              doc.synchronized {
                if (cdk(did) == null)
                  cdk(did) = new S2STraverseMap(Math.min(doc.len(), lDAModel.K))
                cdk(did).inc(topic)
              }
            }

            lDAModel.wtMat.increment(update)

          case null => finish = true
        }
      }

      return true
    }
  }

  /**
    * Train a ML Model
    *
    * @param train : input train data storage
    * @param vali  : validate data storage
    * @return : a learned model
    */
  override def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = ???
}
