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

package com.tencent.angel.spark.ml.kcore

import scala.collection.mutable.ArrayBuffer

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntLongVector, LongIntVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.spark.models.PSVector

class ReIndex(numNodes: Long) extends Serializable {
  private val long2intPsVector: PSVector = {
    PSVector.longKeySparse(-1, numNodes, 1, RowType.T_INT_SPARSE_LONGKEY,
      additionalConfiguration = Map(AngelConf.Angel_PS_PARTITION_CLASS ->
        classOf[ColumnRangePartitioner].getName))
  }
  private val int2longPsVector: PSVector = {
    PSVector.dense(numNodes, 1, RowType.T_LONG_DENSE)
  }

  def train(pairs: Iterator[(Long, Long)]): Unit = {
    val keys = new ArrayBuffer[Long]()
    val values = new ArrayBuffer[Int]()
    pairs.foreach { case (key, value) =>
      keys += key
      values += value.toInt
    }
    val longs = keys.toArray
    val ints2 = values.toArray
    long2intPsVector.update(VFactory.sparseLongKeyIntVector(numNodes.toLong, longs, ints2))
    int2longPsVector.update(VFactory.sparseLongVector(numNodes.toInt, ints2, longs))
  }

  def encode(keys: Array[Long], values: Array[Array[Long]]): (Array[Int], Array[Array[Int]]) = {
    val nodes = (values.flatten ++ keys).distinct
    println(s"num Node = ${nodes.length}")
    val long2int = long2intPsVector.pull(nodes).asInstanceOf[LongIntVector]
    val newKeys = keys.map(long2int.get)
    val newValues = values.map(_.map(long2int.get))
    (newKeys, newValues)
  }

  def decode(nodes: Array[Int]): Array[Long] = {
    val copy = new Array[Int](nodes.length)
    System.arraycopy(nodes, 0, copy, 0, nodes.length)
    int2longPsVector.pull(copy).asInstanceOf[IntLongVector].get(nodes)
  }
}
