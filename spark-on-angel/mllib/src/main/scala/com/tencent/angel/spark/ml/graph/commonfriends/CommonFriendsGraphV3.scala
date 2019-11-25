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

object CommonFriendsGraphV3 {

  def edgeRDD2GraphPartitions(tupleRdd: RDD[(Long, Long)],
                              storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : RDD[CommonFriendsPartitionV3] = {
    tupleRdd.mapPartitions { iter =>
      if (iter.nonEmpty) {
        Iterator.single(new CommonFriendsPartitionV3(iter.toArray))
      } else {
        Iterator.empty
      }
    }
  }
}

class CommonFriendsGraphV3 (@transient val graphParts: RDD[CommonFriendsPartitionV3],
                            cfPSModel: CommonFriendsPSModel) extends Serializable {

  def getNumEdges(): Long = {
    graphParts.map(_.numEdges).sum().toLong
  }

  def run(batchSize: Int): RDD[((Long, Long), Int)] = {
    graphParts.flatMap(_.runEachPartition(cfPSModel, batchSize))
  }

  def save(path: String, cfRDD: RDD[((Int, Int), Int)]): Unit = {
    cfRDD.map { case ((srcNode, dstNode), numFriends) =>
      s"($srcNode,$dstNode), $numFriends "
    }.saveAsTextFile(path)
  }
}
