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

package com.tencent.angel.spark.ml.graph.commonfriends

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


object CommonFriendsGraph{

  def edgeTupleRDD2GraphPartitions(tupleRdd: RDD[(Int, Int)],
                                   model: CommonFriendsPSModel = null,
                                   numPartition: Option[Int] = None,
                                   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : RDD[CommonFriendsPartition] = {

    val partNum = numPartition.getOrElse(tupleRdd.getNumPartitions)
    tupleRdd.groupByKey(partNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        val keys = new ArrayBuffer[Int]()
        val neighbors = new ArrayBuffer[Array[Int]]()
        iter.foreach { case (key, group) =>
          keys += key
          neighbors += group.toArray
        }
        Iterator.single((keys.toArray, neighbors.toArray))
      } else {
        Iterator.empty
      }
    }.map { case (keys, neighbors) =>
      new CommonFriendsPartition(keys, neighbors)
    }.persist(storageLevel)
  }

}


class CommonFriendsGraph(@transient val graph: CommonFriendsGraph,
                         cfPSModel: CommonFriendsPSModel) {

}
