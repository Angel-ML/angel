package com.tencent.angel.spark.ml.tree.util

import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

object ConcurrentUtil {

  private[tree] val DEFAULT_BATCH_SIZE = 1000000

  private[tree] def rangeParallel[A](f: (Int, Int) => A, start: Int, end: Int,
                                           threadPool: ExecutorService,
                                           batchSize: Int = DEFAULT_BATCH_SIZE): Array[Future[A]] = {
    val futures = Array.ofDim[Future[A]](Maths.idivCeil(end - start, batchSize))
    var cur = start
    var threadId = 0
    while (cur < end) {
      val i = cur
      val j = (cur + batchSize) min end
      futures(threadId) = threadPool.submit(new Callable[A] {
        override def call(): A = f(i, j)
      })
      cur = j
      threadId += 1
    }
    futures
  }
}
