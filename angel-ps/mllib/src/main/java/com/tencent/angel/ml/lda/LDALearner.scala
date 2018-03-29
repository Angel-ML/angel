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

import java.io.BufferedOutputStream
import java.util
import java.util.Collections
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.PartitionKey
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.lda.algo.{CSRTokens, Sampler}
import com.tencent.angel.ml.lda.psf.{GetPartFunc, LikelihoodFunc, PartCSRResult}
import com.tencent.angel.ml.math.vector.{DenseIntVector, TIntVector}
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult
import com.tencent.angel.ml.matrix.psf.get.multi.PartitionGetRowsParam
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult
import com.tencent.angel.ml.metric.ObjMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.utils.HdfsUtil
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

  val reqRows = new util.HashMap[Int, util.List[Integer]]()

  for (i <- 0 until pkeys.size()) {
    val pkey = pkeys.get(i)
    val rows = new util.ArrayList[Integer]()
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

  def fetchNk: Unit = {
    val row = model.tMat.getRow(0).asInstanceOf[TIntVector]
    var sum = 0L
    val sb = new mutable.StringBuilder()
    for (i <- 0 until model.K) {
      nk(i) = row.get(i)
      sum += nk(i)
      sb.append(nk(i) + " ")
    }

    LOG.info(sb.toString())
    LOG.info(s"nk_sum=$sum")
  }

  def train(n_iters: Int): Unit = {
    for (epoch <- 1 to n_iters) {
      // One epoch
      fetchNk

      scheduleSample(pkeys)
      ctx.incEpoch()

      // calculate likelihood
      val ll = likelihood
      LOG.info(s"epoch=$epoch local likelihood=$ll")

      // submit to client
      globalMetrics.metric(LOG_LIKELIHOOD, ll)
      //      ctx.incEpoch()

      if (epoch % 4 == 0) reset(epoch)
    }
  }

  def reset(epoch: Int) = {
    LOG.info(s"start reset")
    model.tMat.clock(false).get()
    model.tMat.getRow(0)
    if (ctx.getTaskIndex == 0) {
      model.tMat.zero()
      model.wtMat.zero()
    }
    model.tMat.clock(false).get()
    model.tMat.getRow(0)
    scheduleReset()
    LOG.info(s"finish reset")
  }

  def computeDocllh(): Double = {
    var dkNum = 0
    var ll = 0.0
    for (d <- 0 until data.n_docs) {
      val dk = data.dks(d)
      if (dk != null) {
        dkNum += dk.size
        for (j <- 0 until dk.size)
          ll += Gamma.logGamma(alpha + dk.getVal(j))
        ll -= Gamma.logGamma(data.docLens(d) + alpha * model.K)
      }
    }

    ll -= dkNum * Gamma.logGamma(alpha)
    ll += data.n_docs * Gamma.logGamma(alpha * model.K)
    ll
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

    val ll_doc = scheduleDocllh(data.n_docs)
    LOG.info(s"ll_word_summary=${ll_word_summary} ll_word=${ll_word} ll_doc=${ll_doc}")
    ll = ll_word_summary + ll_word + ll_doc
    ll
  }

  val queue = new LinkedBlockingQueue[Sampler]()
  val executor = Executors.newFixedThreadPool(model.threadNum)

  def scheduleSample(pkeys: java.util.List[PartitionKey]): Boolean = {
    val results = new LinkedBlockingQueue[Future[VoidResult]]()

    class Task(sampler: Sampler, pkey: PartitionKey, csr: PartCSRResult) extends Thread {
      override def run(): Unit = {
        results.add(sampler.sample(pkey, csr))
        queue.add(sampler)
      }
    }

    val client = PSAgentContext.get().getMatrixTransportClient
    val iter = pkeys.iterator()
    val func = new GetPartFunc(null)
    val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()
    val size = pkeys.size
    var idx = Math.min(10, size)
    for (i <- 0 until idx) {
      val pkey = pkeys.get(i)
      val param = new PartitionGetRowsParam(model.wtMat.getMatrixId(), pkey,
        reqRows.get(pkey.getPartitionId))
      val future = client.get(func, param)
      futures.put(pkey, future)
    }

    // copy nk to each sampler
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))

    while (futures.size > 0) {
      val keys = futures.keySet.iterator
      while (keys.hasNext) {
        val pkey = keys.next()
        val future = futures.get(pkey).get
        if (future.isDone) {
          val sampler = queue.take()

          if (idx < size) {
            val pkey = pkeys.get(idx)
            val param = new PartitionGetRowsParam(model.wtMat.getMatrixId(), pkey,
              reqRows.get(pkey.getPartitionId))
            val future = client.get(func, param)
            futures.put(pkey, future)
            idx += 1
          }

          future.get() match {
            case csr: PartCSRResult => executor.execute(new Task(sampler, pkey, csr))
            case _ => throw new AngelException("should by PartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    var error = false
    // calculate the delta value of nk
    // the take means that all tasks have been finished
    val update = new DenseIntVector(model.K)
    for (i <- 0 until model.threadNum) {
      val sampler = queue.take()
      error = sampler.error
      for (i <- 0 until model.K)
        update.plusBy(i, sampler.nk(i) - nk(i))
    }

    model.tMat.increment(0, update)

    for (i <- 0 until pkeys.size()) results.take().get()
    // update for wt
    model.wtMat.clock(false).get()
    // update for nk
    model.tMat.clock().get()

    return error
  }

  def scheduleDocllh(n_docs: Int) = {
    val results = new LinkedBlockingQueue[Double]()
    class Task(index: AtomicInteger) extends Thread {
      private var ll = 0.0

      override def run(): Unit = {
        while (index.get() < n_docs) {
          val d = index.incrementAndGet()
          if (d < n_docs) {
            val dk = data.dks(d)
            for (j <- 0 until dk.size)
              ll += Gamma.logGamma(alpha + dk.getVal(j))
            ll -= Gamma.logGamma(data.docLens(d) + alpha * model.K)
          }
        }
        results.add(ll)
      }
    }

    val index = new AtomicInteger(0)
    var ll = 0.0;
    var nnz = 0
    for (i <- 0 until model.threadNum) executor.execute(new Task(index))
    for (i <- 0 until model.threadNum) ll += results.take()
    for (d <- 0 until n_docs) nnz += data.dks(d).size
    ll -= nnz * Gamma.logGamma(alpha)
    ll += data.n_docs * Gamma.logGamma(alpha * model.K)
    ll
  }

  def scheduleInit(): Unit = {
    val results = new LinkedBlockingQueue[Future[VoidResult]]()
    class Task(sampler: Sampler, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        results.add(sampler.initialize(pkey))
        queue.add(sampler)
      }
    }

    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model))

    var iter = pkeys.iterator()
    while (iter.hasNext) {
      val sampler = queue.take()
      executor.execute(new Task(sampler, iter.next()))
    }

    // calculate the delta value of nk
    // the take means that all tasks have been finished
    val update = new DenseIntVector(model.K)
    for (i <- 0 until model.threadNum) {
      val sampler = queue.take()
      for (i <- 0 until model.K)
        update.plusBy(i, sampler.nk(i) - nk(i))
    }

    model.tMat.increment(0, update)
    // wait for futures
    for (i <- 0 until pkeys.size()) results.take().get()

    // update for wt
    model.wtMat.clock().get()
    // update for nk
    model.tMat.clock().get()
  }

  def scheduleReset(): Unit = {
    val results = new LinkedBlockingQueue[Future[VoidResult]]()
    class Task(sampler: Sampler, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        results.add(sampler.reset(pkey))
        queue.add(sampler)
      }
    }

    util.Arrays.fill(nk, 0)
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))
    val iter = pkeys.iterator()
    while (iter.hasNext) {
      val sampler = queue.take()
      executor.execute(new Task(sampler, iter.next()))
    }

    // calculate the delta value of nk
    // the take means that all tasks have been finished
    val update = new DenseIntVector(model.K)
    for (i <- 0 until model.threadNum) {
      val sampler = queue.take()
      for (i <- 0 until model.K)
        update.plusBy(i, sampler.nk(i) - nk(i))
    }

    model.tMat.increment(0, update)

    for (i <- 0 until pkeys.size()) results.take().get()
    // update for wt
    model.wtMat.clock(false).get()
    // update for nk
    model.tMat.clock().get()
  }

  def inference(n_iters: Int): Unit = {
    for (i <- 1 to n_iters) {
      sampleForInference()
      val ll = scheduleDocllh(data.n_docs)
      LOG.info(s"doc ll = ${ll}")
      globalMetrics.metric(LOG_LIKELIHOOD, ll)
      ctx.incEpoch()
    }
  }

  def initForInference(): Unit = {
    fetchNk
    class Task(sampler: Sampler, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        sampler.initForInference(pkey)
        queue.add(sampler)
      }
    }

    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model))

    val iter = pkeys.iterator()
    while (iter.hasNext) {
      val sampler = queue.take()
      executor.execute(new Task(sampler, iter.next()))
    }

    for (i <- 0 until model.threadNum) queue.take()

    model.tMat.clock(false).get()
    ctx.incEpoch()
  }

  def sampleForInference(): Unit = {
    class Task(sampler: Sampler, pkey: PartitionKey, csr: PartCSRResult) extends Thread {
      override def run(): Unit = {
        sampler.inference(pkey, csr)
        queue.add(sampler)
      }
    }

    val client = PSAgentContext.get().getMatrixTransportClient
    val iter = pkeys.iterator()
    val func = new GetPartFunc(null)
    val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()

    val size = pkeys.size
    var idx = Math.min(10, size)
    for (i <- 0 until idx) {
      val pkey = pkeys.get(i)
      val param = new PartitionGetRowsParam(model.wtMat.getMatrixId(), pkey,
        reqRows.get(pkey.getPartitionId))
      val future = client.get(func, param)
      futures.put(pkey, future)
    }

    // copy nk to each sampler
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))

    while (futures.size > 0) {
      val keys = futures.keySet.iterator
      while (keys.hasNext) {
        val pkey = keys.next()
        val future = futures.get(pkey).get
        if (future.isDone) {
          val sampler = queue.take()

          if (idx < size) {
            val pkey = pkeys.get(idx)
            val param = new PartitionGetRowsParam(model.wtMat.getMatrixId(), pkey,
              reqRows.get(pkey.getPartitionId))
            val future = client.get(func, param)
            futures.put(pkey, future)
            idx += 1
          }

          future.get() match {
            case csr: PartCSRResult => executor.execute(new Task(sampler, pkey, csr))
            case _ => throw new AngelException("should by PartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    for (i <- 0 until model.threadNum) queue.take()

    model.tMat.clock(false).get()
  }


  def initForIncr(): Unit = {

  }

  def sampleForIncr(): Unit = {

  }

  def saveWordTopic(model: LDAModel): Unit = {
    LOG.info("save word topic")
    val dir = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
    val base = dir + "/" + "word_topic"
    val taskId = ctx.getTaskIndex
    val dest = new Path(base, taskId.toString)

    val fs = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    val out = new BufferedOutputStream(fs.create(tmp))


    val num = model.V / ctx.getTotalTaskNum + 1
    val start = taskId * num
    val end = Math.min(model.V, start + num)

    val index = new RowIndex()
    for (i <- start until end) index.addRowId(i)
    val rr = model.wtMat.getRows(index, 1000)

    for (row <- start until end) {
      val x = rr.get(row).get.asInstanceOf[TIntVector]
      val len = x.size()
      val sb = new StringBuilder
      sb.append(x.getRowId + ":")
      for (i <- 0 until len)
        sb.append(s" ${x.get(i)}")
      sb.append("\n")
      out.write(sb.toString().getBytes("UTF-8"))
    }

    out.flush()
    out.close()
    fs.rename(tmp, dest)
  }

  def saveDocTopic(data: CSRTokens, model: LDAModel): Unit = {
    LOG.info("save doc topic ")
    val dir = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
    val base = dir + "/" + "doc_topic"
    val part = ctx.getTaskIndex

    val dest = new Path(base, part.toString)
    val fs = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    val out = new BufferedOutputStream(fs.create(tmp))

    for (d <- 0 until data.dks.size) {
      val sb = new StringBuilder
      val dk = data.dks(d)
      sb.append(data.docIds(d))
      val len = dk.size
      for (i <- 0 until len)
        sb.append(s" ${dk.getKey(i)}:${dk.getVal(i)}")
      sb.append("\n")
      out.write(sb.toString().getBytes("UTF-8"))
    }

    out.flush()
    out.close()
    fs.rename(tmp, dest)
  }
}
