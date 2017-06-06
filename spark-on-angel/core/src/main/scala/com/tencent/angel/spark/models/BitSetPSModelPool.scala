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

package com.tencent.angel.spark.models

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import sun.misc.Cleaner

/**
 * BitSetPSVectorPool is an implement of [[PSModelPool]].
 * BitSetPSVectorPool record the using vector and used vector in `bitSet`, when there is no
 * enough space for new vectors, it will release the used Vector by triggering System.gc.
 */
class BitSetPSModelPool(
    _id: Int,
    _numDimensions: Int,
    _capacity: Int)
  extends PSModelPool(_id, _numDimensions, _capacity) {

  private var size = 0

  private var destroyed = false

  private val bitSet = new java.util.BitSet(capacity)

  private val cleaners = new java.util.WeakHashMap[PSModelProxy, Cleaner]

  private class CleanTask(index: Int) extends Runnable {
    def run(): Unit = {
      bitSet.synchronized {
        bitSet.clear(index)
        size -= 1
      }
    }
  }

  /**
   * Allocate a space for a new vector.
   * It will trigger System.gc to collect the used space when
   * the available pool space is limited
   */
  private[spark] def allocate(): PSModelProxy = {
    if (destroyed) {
      throw new SparkException("This vector pool has been destroyed!")
    }

    bitSet.synchronized {
      if (size < capacity) {
        if (size > math.max(capacity * 0.9, 4)) {
          System.gc()
        }
        val index = bitSet.nextClearBit(0)
        bitSet.set(index)
        size += 1
        return allocate(index)
      }
    }

    System.gc()
    Thread.sleep(100L)

    bitSet.synchronized {
      if (size < capacity) {
        val index = bitSet.nextClearBit(0)
        bitSet.set(index)
        size += 1
        return allocate(index)
      } else {
        throw new SparkException("This vector pool is full!")
      }
    }
  }

  private def allocate(index: Int): PSModelProxy = {
    val task = new CleanTask(index)
    val key = new PSModelProxy(this, id, index, numDimensions)
    cleaners.put(key, Cleaner.create(key, task))
    key
  }

  private[spark] def delete(key: PSModelProxy): Unit = {
    cleaners.remove(key).clean()
  }

  private[spark] def destroy(): Unit = {
    destroyed = true
    cleaners.asScala.keys.toArray.foreach(_.delete())
  }

}
