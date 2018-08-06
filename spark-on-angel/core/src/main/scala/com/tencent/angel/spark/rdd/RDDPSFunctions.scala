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

package com.tencent.angel.spark.rdd

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.models.vector.cache.PushMan

class RDDPSFunctions[T: ClassTag](self: RDD[T]) extends Serializable {

  /**
   * psAggregate is similar to RDD.aggregate, you can update the PSVector in `seqOp` at the same
   * time of aggregation.
   *
   * @param zeroValue the initial value
   * @param seqOp an operator used to accumulate results within a partition
   * @param combOp an associative operator used to combine results from different partitions
   */
  def psAggregate[U: ClassTag](zeroValue: U)(
      seqOp: (U, T) => U,
      combOp: (U, U) => U): U = {
    val res = self.mapPartitions { iter =>
      val result = iter.foldLeft(zeroValue)(seqOp)
      Iterator(result)
    }.reduce(combOp)
    PushMan.flushAll()
    res
  }

  def psFoldLeft[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U): U = {
    val res = self.mapPartitions { iter =>
      val result = iter.foldLeft(zeroValue)(seqOp)
      Iterator(result)
    }.collect().head
    PushMan.flushAll()
    res
  }

}

object RDDPSFunctions {

  /** Implicit conversion from an RDD to RDDFunctions. */
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): RDDPSFunctions[T] = {
    new RDDPSFunctions[T](rdd)
  }

}
