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
package com.tencent.angel.spark.ml.graph.utils

import com.tencent.angel.spark.ml.graph.utils.collection.OpenHashSet
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object DistinctNodes {

  def apply[T: ClassTag](edges: RDD[(T, T)], partitionNum: Int): RDD[T] = {
    val temp = edges.mapPartitions { iter =>
      val node = new OpenHashSet[T]()
      iter.foreach { x =>
        node.add(x._1)
        node.add(x._2)
      }
      node.iterator
    }.map(x => (x, 1)).partitionBy(new HashPartitioner(partitionNum))
    temp.mapPartitions { iter =>
      val node = new OpenHashSet[T]()
      iter.foreach { x =>
        node.add(x._1)
      }
      node.iterator
    }
  }

  def apply[T: ClassTag, U: ClassTag](edges: RDD[(T, T, U)], partitionNum: Int): RDD[T] = {
    val temp = edges.mapPartitions { iter =>
      val node = new OpenHashSet[T]()
      iter.foreach { x =>
        node.add(x._1)
        node.add(x._2)
      }
      node.iterator
    }.map(x => (x, 1)).partitionBy(new HashPartitioner(partitionNum))
    temp.mapPartitions { iter =>
      val node = new OpenHashSet[T]()
      iter.foreach { x =>
        node.add(x._1)
      }
      node.iterator
    }
  }

  def fromWeightedEdge[T: ClassTag, U: ClassTag](edges: RDD[(T, (T, U))], partitionNum: Int): RDD[T] = {
    val temp = edges.mapPartitions { iter =>
      val node = new OpenHashSet[T]()
      iter.foreach { x =>
        node.add(x._1)
        node.add(x._2._1)
      }
      node.iterator
    }.map(x => (x, 1)).partitionBy(new HashPartitioner(partitionNum))
    temp.mapPartitions { iter =>
      val node = new OpenHashSet[T]()
      iter.foreach { x =>
        node.add(x._1)
      }
      node.iterator
    }
  }
}
