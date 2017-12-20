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
 */

package com.tencent.angel.spark.context

import org.apache.spark.SparkException
import sun.misc.Cleaner

import com.tencent.angel.spark.models.vector.VectorType.VectorType
import com.tencent.angel.spark.models.vector.enhanced.PullMan
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector, SparsePSVector, VectorType}

/**
 * PSVectorPool delegate a memory space on PS servers,
 * which hold `capacity` number vectors with `numDimensions` dimension.
 * The dimension of PSVectors in one PSVectorPool is the same.
 *
 * A PSVectorPool is like a Angel Matrix.
 *
 * @param id PSVectorPool unique id
 * @param dimension Dimension of vectors
 * @param capacity Capacity of pool
 */

private[spark] class PSVectorPool(
    val id: Int,
    val dimension: Long,
    val capacity: Int,
    val vType: VectorType) {

  val cleaners = new java.util.WeakHashMap[PSVector, Cleaner]
  val bitSet = new java.util.BitSet(capacity)
  var destroyed = false
  var size = 0

  private[spark] def allocate(): PSVector = {
    if (destroyed) {
      throw new SparkException("This vector pool has been destroyed!")
    }

    if (size > math.max(capacity * 0.9, 4)) {
      System.gc()
    }

    tryOnce match {
      case Some(toReturn) => return toReturn
      case None =>
    }

    System.gc()
    Thread.sleep(100L)

    // Try again
    tryOnce match {
      case Some(toReturn) => return toReturn
      case None => throw new SparkException("This vector pool is full!")
    }

  }

  private def tryOnce: Option[PSVector] = {
    bitSet.synchronized {
      if (size < capacity) {
        val index = bitSet.nextClearBit(0)
        bitSet.set(index)
        size += 1
        return Some(doCreateOne(index))
      }
    }
    None
  }

  private def doCreateOne(index: Int): PSVector = {
    val vector = if (vType == VectorType.SPARSE) {
      new SparsePSVector(id, index, dimension)
    } else {
      new DensePSVector(id, index, dimension)
    }
    val task = new CleanTask(this.id, index)
    cleaners.put(vector, Cleaner.create(vector, task))
    vector
  }

  private class CleanTask(poolId: Int, index: Int) extends Runnable {
    def run(): Unit = {
      bitSet.synchronized {
        bitSet.clear(index)
        size -= 1
      }
      PullMan.autoRelease(poolId, index)
    }
  }

  private[spark] def delete(key: PSVector): Unit = {
    cleaners.remove(key).clean()
  }

  private[spark] def destroy(): Unit = {
    destroyed = true
  }

  /**
    * Make sure dimension compatible
    */
  private def assertCompatible(other: Array[Double]): Unit = {
    if (this.dimension != other.length) {
      throw new SparkException(s"The target array's dimension " +
        s"does not match this vector pool! \n" +
        s"pool dimension is $dimension," +
        s"but target array's dimension is ${other.length}")
    }
  }

}

object PSVectorPool {
  val DEFAULT_POOL_CAPACITY = 10
}