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

package org.apache.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionsUtils
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet}
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SparkPrivateClassProxy {

  def makeBinarySearch[K: Ordering : ClassTag]: (Array[K], K) => Int = {
    CollectionsUtils.makeBinarySearch
  }

  def sketch[K: ClassTag](rdd: RDD[K],
                          sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    RangePartitioner.sketch(rdd, sampleSizePerPartition)
  }

  def determineBounds[K: Ordering : ClassTag](
                                               candidates: ArrayBuffer[(K, Float)],
                                               partitions: Int): Array[K] = {
    RangePartitioner.determineBounds(candidates, partitions)
  }

  def getXORShiftRandom(seed: Long): XORShiftRandom = new XORShiftRandom(seed)

  def createOpenHashMap[K: ClassTag, V: ClassTag](): OpenHashMap[K, V] = {
    new OpenHashMap[K, V]()
  }

  def createOpenHashSet[K: ClassTag](): OpenHashSet[K] = {
    new OpenHashSet[K]()
  }
}
