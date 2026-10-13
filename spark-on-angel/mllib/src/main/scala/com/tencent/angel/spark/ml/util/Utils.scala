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

package com.tencent.angel.spark.ml.util


import spire.ClassTag

import scala.collection.mutable.ArrayBuffer

object Utils {
  def iteratorBatch[D: ClassTag](iterator: Iterator[D], func: Array[D] => Unit, batchSize: Int = 10000): Unit = {
    var batch = new ArrayBuffer[D](batchSize)
    while (iterator.hasNext) {
      val nx = iterator.next()
      batch.append(nx)
      if (batch.size >= batchSize || !iterator.hasNext) {
        func(batch.toArray)
        batch = new ArrayBuffer[D](batchSize)
      }
    }
  }

  def runOnTimer[O: ClassTag](func: () => O): (O, Long) = {
    val start = System.currentTimeMillis()
    val result = func()
    (result, System.currentTimeMillis() - start)
  }

  def getBatchInterval(total: Long, batch: Long, expect: Long = 10): Long = {
    val number = if (total % batch == 0) {
      total / batch
    } else {
      total / batch + 1
    }
   math.max(number / expect, 1)
  }

  def secondTimer(cost: Long, truncation: Long = 3): String = {
    (cost / 1000.0).formatted(s"%.${truncation}f")
  }

}

