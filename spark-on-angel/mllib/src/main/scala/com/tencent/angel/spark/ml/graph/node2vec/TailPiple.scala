package com.tencent.angel.spark.ml.graph.node2vec

import java.util.concurrent._

import com.tencent.angel.graph.client.node2vec.getfuncs.pullpathtail.{PullPathTail, PullPathTailParam, PullPathTailResult}
import com.tencent.angel.graph.client.node2vec.updatefuncs.pushpathtail.{PushPathTail, PushPathTailParam}
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.{Long2LongOpenHashMap, Long2ObjectOpenHashMap}


class TailPuller(walkPath: PSMatrix, workPartId: Int, batchSize: Int = 1000) extends Runnable {
  private val queue = new ArrayBlockingQueue[Long2ObjectOpenHashMap[Array[Long]]](5)
  private val executor = Executors.newSingleThreadExecutor
  private var stopFlag = false
  private var future: Future[_] = _

  def start(): Unit = {
    future = executor.submit(this)
  }

  override def run(): Unit = {
    while (!stopFlag && !Thread.interrupted()) {
      // println("begin to do pull batch")
      val func = new PullPathTail(new PullPathTailParam(walkPath.id, workPartId, batchSize))
      val pullFuture = walkPath.asyncPsfGet(func)

      while (!stopFlag && !pullFuture.isDone && !Thread.interrupted()) {
        try {
          val pulled = pullFuture.get(500, TimeUnit.MILLISECONDS)
            .asInstanceOf[PullPathTailResult].getResult
          if (pulled != null) {
            // println("put the pulled data to the queue")
            queue.put(pulled)
          }
        } catch {
          case e: TimeoutException =>
          // e.printStackTrace()
          case e: InterruptedException =>
            e.printStackTrace()
            pullFuture.cancel(true)
          case e: ExecutionException =>
            e.printStackTrace()
            pullFuture.cancel(true)
        }

        if (!pullFuture.isDone) {
          pullFuture.cancel(true)
        }

        // println("finished to do pull batch")
      }
    }
  }

  def shutDown(): Unit = {
    stopFlag = true
    executor.shutdown()
  }

  def pull(): Long2ObjectOpenHashMap[Array[Long]] = {
    // println("get the pulled data")
    queue.take()
  }

}

class TailPusher(walkPath: PSMatrix) extends Runnable {
  private val queue = new LinkedBlockingQueue[Long2LongOpenHashMap]()
  private val executor = Executors.newSingleThreadExecutor
  private var stopFlag = false
  private var future: Future[_] = _
  private val timeout: Long = 500

  def start(): Unit = {
    future = executor.submit(this)
  }

  override def run(): Unit = {
    while (!stopFlag && !Thread.interrupted()) {
      // println("get data to be pushed from queue")
      var data: Long2LongOpenHashMap = null
      while (!stopFlag && !Thread.interrupted() && data == null) {
        data = queue.poll(timeout, TimeUnit.MILLISECONDS)
      }

      if (data != null && !data.isEmpty) {
        // println("begin to do push batch, size = " + data.size())
        val func = new PushPathTail(new PushPathTailParam(walkPath.id, data))
        val pushFuture = walkPath.asyncPsfUpdate(func)

        while (!stopFlag && !pushFuture.isDone && !Thread.interrupted()) {
          try {
            pushFuture.get(timeout, TimeUnit.MILLISECONDS)
          } catch {
            case e: TimeoutException =>
            // e.printStackTrace()
            case e: InterruptedException =>
              e.printStackTrace()
              pushFuture.cancel(true)
            case e: ExecutionException =>
              e.printStackTrace()
              pushFuture.cancel(true)
          }

          if (!pushFuture.isDone) {
            pushFuture.cancel(true)
          }

        }
        // println("finished to do push batch")
      }

      data = null
    }
  }

  def shutDown(): Unit = {
    stopFlag = true
    executor.shutdown()
  }

  def push(ele: Long2LongOpenHashMap): Unit = {
    // println("push batch to the queue, size = " + ele.size())
    queue.put(ele)
  }

}
