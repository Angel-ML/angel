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

package com.tencent.angel.graph

import com.tencent.angel.graph.utils.element.{GraphStats, NeighborTable, NeighborTablePartition}
import com.tencent.angel.graph.utils.element.Element.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

object GraphOps {

  def loadEdges(dataset: Dataset[_], srcNodeIdCol: String, dstNodeIdCol: String): RDD[(VertexId, VertexId)] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
      .filter(row => !row.anyNull)
      .mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1)) {
            Iterator.empty
          } else {
            Iterator.single((row.getLong(0), row.getLong(1)))
          }
        }
      }
  }

  def edgesToNeighborTable(edges: RDD[(Long, Long)], partitionNum: Int): RDD[NeighborTable[Object]] = {
    edges.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(NeighborTable(src, group.toArray.distinct.filter(_ != src).sorted))
        }
      } else {
        Iterator.empty
      }
    }
  }


  def buildNeighborTablePartition[ED: ClassTag](data: RDD[NeighborTable[ED]],
                                                isDirected: Boolean = false): RDD[NeighborTablePartition[ED]] = {
    NeighborTablePartition.fromNeighborTableRDD(data, isDirected)
  }

  def getStats[ED: ClassTag](data: RDD[NeighborTablePartition[ED]]): GraphStats = {
    data.map(_.stats).reduce(_ + _)
  }

}
