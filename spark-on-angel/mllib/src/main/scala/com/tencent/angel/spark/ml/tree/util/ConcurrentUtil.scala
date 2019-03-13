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
