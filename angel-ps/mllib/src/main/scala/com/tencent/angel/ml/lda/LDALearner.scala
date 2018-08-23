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


package com.tencent.angel.ml.lda

import java.io.BufferedOutputStream
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList}
import java.util.Collections
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.PartitionKey
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.MLLearner
import com.tencent.angel.ml.core.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.lda.algo.{CSRTokens, Sampler}
import com.tencent.angel.ml.lda.psf._
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.IntIntVector
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult
import com.tencent.angel.ml.matrix.psf.get.getrows.PartitionGetRowsParam
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.matrix.psf.update.zero.Zero.ZeroParam
import com.tencent.angel.ml.metric.ObjMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.utils.{HdfsUtil, MemoryUtils}
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.commons.math.special.Gamma
import org.apache.hadoop.fs.Path

import scala.collection.mutable

class LDALearner(ctx: TaskContext, model: LDAModel, data: CSRTokens) extends MLLearner(ctx) {

  val LOG = LogFactory.getLog(classOf[LDALearner])

  val pkeys = PSAgentContext.get().getMatrixMetaManager.
    getPartitions(model.wtMat.getMatrixId())

  LOG.info(s"max doc len = ${data.maxDocLen}")
  LOG.info(s"size for pkeys = ${MemoryUtils.estimateMemorySize(pkeys)}")
  LOG.info(s"size for data = ${MemoryUtils.estimateMemorySize(data)}")
  LOG.info(s"size for sampler = ${MemoryUtils.estimateMemorySize(new Sampler(data, model))}")

  val reqRows = new JHashMap[Int, JList[Integer]]()

  for (i <- 0 until pkeys.size()) {
    val pkey = pkeys.get(i)
    val rows = new JArrayList[Integer]()
    for (w <- pkey.getStartRow until pkey.getEndRow) {
      if (data.ws(w + 1) - data.ws(w) > 0) rows.add(w)
    }
    reqRows.put(pkey.getPartitionId, rows)
  }

  Collections.shuffle(pkeys)

  // Hyper parameters
  val alpha = model.alpha
  val beta = model.beta
  val lgammaBeta = Gamma.logGamma(beta)
  val lgammaAlpha = Gamma.logGamma(alpha)
  val lgammaAlphaSum = Gamma.logGamma(alpha * model.K)

  val nk = new Array[Int](model.K)

  globalMetrics.addMetric(LOG_LIKELIHOOD, new ObjMetric())


  /**
    * Train a ML Model
    *
    * @param train : input train data storage
    * @param vali  : validate data storage
    * @return : a learned model
    */
  override
  def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = ???


  def initialize(): Unit = {
    scheduleInit()
    LOG.info("init finish")

    ctx.incEpoch()

    val ll = likelihood
    LOG.info(s"ll=${ll}")
    globalMetrics.metric(LOG_LIKELIHOOD, ll)
    ctx.incEpoch()
  }

  def fetchNk(): Unit = {
    val row = model.tMat.getRow(0).asInstanceOf[IntIntVector]
    val values = row.getStorage.getValues
    System.arraycopy(values, 0, nk, 0, nk.length)
    LOG.info(s"sum of nk (total tokens) = ${row.sum()}")
  }

  def train(n_iters: Int): Unit = {
    for (epoch <- 1 to n_iters) {
      // One epoch
      // First fetch topic
      fetchNk()
      // Sample all partitions
      sample()
      ctx.incEpoch()

      // calculate likelihood
      val ll = likelihood
      LOG.info(s"epoch=$epoch local likelihood=$ll")

      // submit to client
      globalMetrics.metric(LOG_LIKELIHOOD, ll)

      // Reset word-topic matrix every four epoches for one reason:
      // If one server is failed (very pervasive), Angel would reallocate a new one.
      // Then there will be data inconsistency between word-topic and
      // the topic assignment in workers.
      if (epoch % 4 == 0) reset(epoch)
    }
  }

  def reset(epoch: Int) = {
    LOG.info(s"start reset")
    model.tMat.clock(false).get()
    model.tMat.getRow(0)
    if (ctx.getTaskIndex == 0) {
      model.tMat.zero()
      ctx.getMatrix(model.tMat.modelName).update(new ResetFunc(new ZeroParam(model.tMat.getMatrixId(), false)))
      ctx.getMatrix(model.wtMat.modelName).update(new ResetFunc(new ZeroParam(model.wtMat.getMatrixId(), false)))
    }
    model.tMat.clock(false).get()
    model.tMat.getRow(0)
    scheduleReset()
    LOG.info(s"finish reset")
  }

  def computeWordLLHSummary: Double = {
    var ll = model.K * Gamma.logGamma(beta * model.V)
    for (k <- 0 until model.K)
      ll -= Gamma.logGamma(nk(k) + beta * model.V)
    ll
  }

  def computeWordLLH: Double = {
    model.wtMat.get(new LikelihoodFunc(model.wtMat.getMatrixId(), beta)) match {
      case r: ScalarAggrResult => r.getResult
      case _ => throw new AngelException("should be ScalarAggrResult")
    }
  }

  def likelihood: Double = {
    var ll = 0.0
    fetchNk
    val ll_word_summary = if (ctx.getTaskIndex == 0) computeWordLLHSummary else 0
    val ll_word = if (ctx.getTaskIndex == 0) computeWordLLH else 0
    val ll_doc = docLLH(data.n_docs)
    LOG.info(s"ll_word_summary=${ll_word_summary} ll_word=${ll_word} ll_doc=${ll_doc}")
    ll = ll_word_summary + ll_word + ll_doc
    ll
  }

  val queue = new LinkedBlockingQueue[Sampler]()
  val executor = Executors.newFixedThreadPool(model.threadNum)
  val results = new LinkedBlockingQueue[Future[VoidResult]]()

  object SampleOps extends Enumeration {
    val INIT, SAMPLE, RESET, INIT_FOR_INFERENCE, INFERENCE = Value
  }

  class Task(sampler: Sampler, pkey: PartitionKey, csr: PartCSRResult, Op: SampleOps.Value) extends Thread {
    override def run(): Unit = {
      Op match {
        case SampleOps.INIT =>
          results.add(sampler.initialize(pkey))
        case SampleOps.SAMPLE =>
          results.add(sampler.sample(pkey, csr))
        case SampleOps.RESET =>
          results.add(sampler.reset(pkey))
        case SampleOps.INIT_FOR_INFERENCE =>
          sampler.initForInference(pkey)
        case SampleOps.INFERENCE =>
          sampler.inference(pkey, csr)
      }
      queue.add(sampler)
    }
  }

  def scheduleWithFetch(pkeys: JList[PartitionKey],
                        update: Boolean,
                        Op: SampleOps.Value): Unit = {

    // Preparation for fetching
    val client = PSAgentContext.get().getMatrixTransportClient
    val func = new GetPartFunc(null)
    val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()

    // First fetching some partitions (min(thread, size))
    val size = pkeys.size
    var idx = Math.min(model.threadNum, size)
    for (i <- 0 until idx) {
      val pkey = pkeys.get(i)
      val param = new PartitionGetRowsParam(model.wtMat.getMatrixId(), pkey,
        reqRows.get(pkey.getPartitionId))
      val future = client.get(func, param)
      futures.put(pkey, future)
    }

    // Copy nk to each sampler and allocate some samplers to work
    for (i <- 0 until model.threadNum)
      queue.add(new Sampler(data, model).set(nk))

    // Traverse all request, Sample one partition if it has been fetched
    while (futures.size > 0) {
      val keys = futures.keySet.iterator
      while (keys.hasNext) {
        val pkey = keys.next()
        val future = futures.get(pkey).get
        if (future.isDone) {
          // Find one is fetched, waiting for a sampler once it is free
          val sampler = queue.take()
          // If has remaining partitions, issuing the fetch request
          if (idx < size) {
            val nextPkey = pkeys.get(idx)
            val param = new PartitionGetRowsParam(model.wtMat.getMatrixId(), nextPkey,
              reqRows.get(nextPkey.getPartitionId))
            val future = client.get(func, param)
            futures.put(nextPkey, future)
            idx += 1
          }

          // Sample the fetched partition
          future.get() match {
            case csr: PartCSRResult => executor.execute(new Task(sampler, pkey, csr, Op))
            case _ => throw new AngelException("should by PartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    if (update) {
      // Calculate the delta value of nk
      // The take means that all tasks have been finished
      val update = VFactory.denseIntVector(model.K)
      for (i <- 0 until model.threadNum) {
        val sampler = queue.take()
        for (i <- 0 until model.K)
          update.set(i, update.get(i) + sampler.nk(i) - nk(i))
      }

      // Update topic matrix
      model.tMat.increment(0, update)

      // Waiting for all update request finishing
      for (i <- 0 until pkeys.size())
        results.take().get()

      // update the clock for wt (this can be removed)
      model.wtMat.clock(false).get()
    } else {
      // Wait for all samplers finishing
      for (i <- 0 until model.threadNum)
        queue.take()
    }

    // Update, and increase the clock for topic matrix
    model.tMat.clock().get()
  }

  def scheduleWithoutFetch(pkeys: JList[PartitionKey],
                           update: Boolean,
                           Op: SampleOps.Value): Unit = {
    // Allocate some samplers to work
    java.util.Arrays.fill(nk, 0)
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))

    val iter = pkeys.iterator()
    while (iter.hasNext) {
      val sampler = queue.take()
      executor.execute(new Task(sampler, iter.next(), null, Op))
    }

    if (update) {
      // calculate the delta value of nk
      // the take means that all tasks have been finished
      val update = VFactory.denseIntVector(model.K)
      for (i <- 0 until model.threadNum) {
        val sampler = queue.take()
        for (i <- 0 until model.K)
          update.set(i, update.get(i) + sampler.nk(i) - nk(i))
      }

      model.tMat.increment(0, update)
      // wait for futures
      for (i <- 0 until pkeys.size()) results.take().get()

      // update for wt
      model.wtMat.clock(false).get()
    } else {
      for (i <- 0 until model.threadNum)
        queue.take()
    }

    // update for nk
    model.tMat.clock().get()
  }

  def sample(): Unit = {
    scheduleWithFetch(pkeys, true, SampleOps.SAMPLE)
  }

  def sampleForInference(): Unit = {
    scheduleWithFetch(pkeys, false, SampleOps.INFERENCE)
  }

  def docLLH(n_docs: Int): Double = {
    val results = new LinkedBlockingQueue[Double]()
    class Task(index: AtomicInteger) extends Thread {
      private var ll = 0.0
      private val tk = new Array[Int](model.K)

      override def run(): Unit = {
        while (index.get() < n_docs) {
          val d = index.incrementAndGet()
          if (d < n_docs) {
            java.util.Arrays.fill(tk, 0)
            var i = data.ds(d)
            while (i < data.ds(d + 1)) {
              tk(data.topics(i)) += 1
              i += 1
            }

            for (j <- 0 until model.K) {
              if (tk(j) > 0)
                ll += Gamma.logGamma(alpha + tk(j)) - lgammaAlpha
            }

            ll -= Gamma.logGamma(data.docLens(d) + alpha * model.K) - lgammaAlphaSum
          }
        }
        results.add(ll)
      }
    }

    val index = new AtomicInteger(0)
    var ll = 0.0;
    for (i <- 0 until model.threadNum) executor.execute(new Task(index))
    for (i <- 0 until model.threadNum) ll += results.take()
    ll
  }

  def scheduleInit(): Unit = {
    scheduleWithoutFetch(pkeys, true, SampleOps.INIT)
  }

  def scheduleReset(): Unit = {
    scheduleWithoutFetch(pkeys, true, SampleOps.RESET)
  }

  def inference(n_iters: Int): Unit = {
    for (i <- 1 to n_iters) {
      sampleForInference()
      val ll = docLLH(data.n_docs)
      LOG.info(s"doc ll = ${ll}")
      globalMetrics.metric(LOG_LIKELIHOOD, ll)
      ctx.incEpoch()
    }
  }

  def initForInference(): Unit = {
    fetchNk
    scheduleWithoutFetch(pkeys, false, SampleOps.INIT_FOR_INFERENCE)
  }

  // Saving functions

  def createDestAndTmpFile(matrix: String, dir: String): (Path, Path) = {
    val taskId = ctx.getTaskIndex
    val base = dir + "/" + matrix
    val dest = new Path(base, taskId.toString)
    val fs = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    return (dest, tmp)
  }

  def saveWordTopic(model: LDAModel): Unit = {
    LOG.info("save word topic")
    val taskId = ctx.getTaskIndex

    val num = model.V / ctx.getTotalTaskNum + 1
    val start = taskId * num
    val end = Math.min(model.V, start + num)

    if (start < end) {
      val index = new RowIndex()
      for (i <- start until end) index.addRowId(i)

      val dir = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
      val base = dir + "/" + "word_topic"
      val dest = new Path(base, taskId.toString)

      val fs = dest.getFileSystem(conf)
      val tmp = HdfsUtil.toTmpPath(dest)
      val out = new BufferedOutputStream(fs.create(tmp))

      val rr = model.wtMat.getRows(index, 1000)

      for (row <- start until end) {
        val x = rr.get(row).get.asInstanceOf[IntIntVector]
        val sb = new StringBuilder
        sb.append(x.getRowId)
        if (x.isSparse) {
          val iter = x.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            val k = entry.getIntKey
            val v = entry.getIntValue
            if (v > 0)
              sb.append(s" ${k}:${v}")
          }
        } else {
          val values = x.getStorage.getValues
          for (i <- 0 until values.length) {
            if (values(i) > 0)
              sb.append(s" ${i}:${values(i)}")
          }
        }
        sb.append("\n")
        out.write(sb.toString().getBytes("UTF-8"))
      }

      out.flush()
      out.close()
      fs.rename(tmp, dest)
    }
  }

  def saveWordTopicDistribution(model: LDAModel, start: Int, end: Int, num: Int): Unit = {
    val taskId = ctx.getTaskIndex
    val index = new JArrayList[Integer]()
    index.add(start)
    index.add(end)
    val dir = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
    val base = dir + "/" + "topic_word_distribution"
    val dest = new Path(base, taskId.toString + "_" + num)

    val fs = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    val out = new BufferedOutputStream(fs.create(tmp))

    val param = new GetColumnParam(model.wtMat.getMatrixId(), index)
    val func = new GetColumnFunc(param)
    val result = model.wtMat.get(func).asInstanceOf[ColumnGetResult]

    fetchNk

    var sum: Long = 0L
    val cks = result.cks
    val keyIterator = cks.keySet().iterator()
    while (keyIterator.hasNext) {
      val column = keyIterator.next()
      val sb = new StringBuilder
      sb.append(column)
      val ck = cks.get(column)
      val numTopicSum = nk(column.toInt)
      val iter = ck.int2IntEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val row = entry.getIntKey
        val value = entry.getIntValue
        sum += value
        val p = (value + model.beta) / (numTopicSum + model.V * model.beta)
        sb.append(s" ${row}:${p}")
      }
      sb.append("\n")
      out.write(sb.toString().getBytes("UTF-8"))
    }

    out.flush()
    out.close()
    fs.rename(tmp, dest)
    LOG.info(s"sum = $sum")
  }

  def saveWordTopicDistribution(model: LDAModel): Unit = {
    LOG.info("save word topic distribution")
    val taskId = ctx.getTaskIndex

    val num = model.K / ctx.getTotalTaskNum + 1
    val start = taskId * num
    val end = Math.min(model.K, start + num)

    val step = 100
    var i = start
    var cnt = 0
    while (i < end) {
      saveWordTopicDistribution(model, i, Math.min(i + step, end), cnt)
      i = i + step
      cnt += 1
    }
  }

  def saveDocTopic(dir: String, data: CSRTokens, model: LDAModel): Unit = {
    LOG.info("save doc topic to " + dir)
    val base = dir + "/" + "doc_topic"
    val part = ctx.getTaskIndex

    val dest = new Path(base, part.toString)
    val fs = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    val out = new BufferedOutputStream(fs.create(tmp))

    val tk = new Array[Int](model.K)

    for (d <- 0 until data.n_docs) {
      val sb = new StringBuilder
      java.util.Arrays.fill(tk, 0)
      var i = data.ds(d)
      while (i < data.ds(d + 1)) {
        tk(data.topics(i)) += 1
        i += 1
      }

      sb.append(data.docIds(d))
      for (j <- 0 until model.K) {
        if (tk(j) > 0) {
          sb.append(s" ${j}:${tk(j)}")
        }
      }
      sb.append("\n")
      out.write(sb.toString().getBytes("UTF-8"))
    }

    out.flush()
    out.close()
    fs.rename(tmp, dest)
  }

  def saveDocTopicDistribution(dir: String, data: CSRTokens, model: LDAModel): Unit = {
    LOG.info("save doc topic distribution to " + dir)
    val base = dir + "/" + "doc_topic_distribution"
    val part = ctx.getTaskIndex

    val dest = new Path(base, part.toString)
    val fs = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    val out = new BufferedOutputStream(fs.create(tmp))

    val tk = new Array[Int](model.K)

    for (d <- 0 until data.n_docs) {
      val sb = new StringBuilder
      java.util.Arrays.fill(tk, 0)
      var i = data.ds(d)
      while (i < data.ds(d + 1)) {
        tk(data.topics(i)) += 1
        i += 1
      }
      sb.append(data.docIds(d))
      val num = data.docLens(d)

      for (j <- 0 until model.K) {
        if (tk(j) > 0) {
          val value = (tk(j) + model.alpha) / (num + model.K * model.alpha)
          sb.append(s" ${j}:${value}")
        }
      }

      sb.append("\n")
      out.write(sb.toString().getBytes("UTF-8"))
    }

    out.flush()
    out.close()
    fs.rename(tmp, dest)

  }
}
