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

import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntIntVector, IntLongVector}
import com.tencent.angel.ml.matrix.{MatrixContext, PartContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Node Indexer, encode from Long to Int and decode from Int to Long
  */
class NodeIndexer extends Serializable {

  import NodeIndexer._

  private var long2int: PSVector = _
  private var int2long: PSVector = _
  private var numPSPartition: Int = -1
  private var numNodes: Int = -1

  def getNumNodes: Int = {
    assert(numNodes > 0, "num of nodes should greater than 0")
    numNodes
  }

  def train(numPSPartition: Int, nodes: RDD[Long], batchSize: Int = 1000000): Unit = {
    this.numPSPartition = numPSPartition
    val maxId = nodes.max() + 1
    val minId = nodes.min()
    val nodesNum = nodes.count()
    nodes.persist(StorageLevel.DISK_ONLY)

    // create ps for encoder mapping
    val ctx = new MatrixContext(LONG2INT, 1, minId, maxId)
    ctx.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    LoadBalancePartitioner.partition(nodes, maxId, numPSPartition, ctx)
    this.long2int = new PSVectorImpl(PSMatrixUtils.createPSMatrix(ctx),
      0, maxId, RowType.T_INT_SPARSE_LONGKEY)

    val intRange = Int.MaxValue.toLong - Int.MinValue.toLong + 1l
    val exceedNum = nodesNum - intRange
    assert(exceedNum <= 0, s"nodesNum exceeds intRange: $nodesNum vs $intRange, could not trans nodeId to int type.")
    val offset = if (nodesNum > Int.MaxValue) nodesNum - Int.MaxValue else 0
    //create Long to Int mapping
    val nodeIndex = nodes.map((_, null)).sortByKey().zipWithIndex().map(x => (x._1._1, x._2 - offset))
    this.numNodes = nodesNum.toInt

    nodes.unpersist(false)

    // create ps for decoder mapping
    val ctx2 = new MatrixContext(INT2LONG, 1, this.numNodes)
    ctx2.setRowType(RowType.T_LONG_DENSE)
    nodeIndex.mapPartitions { iter =>
      val first = iter.next()._2
      val last = iter.toArray.last._2
      Iterator.single((first, last))
    }.collect().foreach { case (start, end) =>
      ctx2.addPart(new PartContext(0, 1, start, end + 1L, (end - start + 1).toInt))
    }
    this.int2long = new PSVectorImpl(PSMatrixUtils.createPSMatrix(ctx2),
      0, numNodes + 1, RowType.T_LONG_DENSE)

    // update mapping to ps
    nodeIndex.foreachPartition { iter =>
      BatchIter(iter, batchSize).foreach { batch =>
        val (key, value) = batch.unzip
        val intValues = value.map(_.toInt)
        val long2intVec = VFactory.sparseLongKeyIntVector(Long.MaxValue, key, intValues)
        val int2longVec = VFactory.sparseLongVector(this.numNodes, intValues, key)
        long2int.update(long2intVec)
        int2long.update(int2longVec)
      }
    }
    nodeIndex.unpersist(false)
  }

  def encode[C: ClassTag, U: ClassTag](rdd: RDD[C], batchSize: Int)(
    func: (Array[C], PSVector) => Iterator[U]): RDD[U] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        func(batch, long2int)
      }
    }
  }

  def destroyEncoder(): Unit = {
    val master = PSAgentContext.get().getMasterClient
    master.releaseMatrix(LONG2INT)
    long2int = null
  }

  def decode[C: ClassTag, U: ClassTag](rdd: RDD[C],
                                       func: (Array[C], PSVector) => Iterator[U],
                                       batchSize: Int): RDD[U] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        func(batch, int2long)
      }
    }
  }

  def decode[C: ClassTag, U: ClassTag](rdd: RDD[C], batchSize: Int)(
    func: (Array[C], PSVector) => Iterator[U]): RDD[U] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        func(batch, int2long)
      }
    }
  }

  def decodePartition[C: ClassTag, U: ClassTag](rdd: RDD[C])(func: PSVector => Iterator[C] => Iterator[U]): RDD[U] = {
    rdd.mapPartitions(func(int2long))
  }

  def decodeInt2IntPSVector(ps: PSVector): RDD[(Long, Long)] = {
    val sc = SparkContext.getOrCreate()
    val master = PSAgentContext.get().getMasterClient
    val partitions = master.getMatrix(INT2LONG)
      .getPartitionMetas.map { case (_, p) =>
      (p.getStartCol.toInt, p.getEndCol.toInt)
    }.toSeq
    sc.parallelize(partitions, this.numPSPartition).flatMap { case (start, end) =>
      val intKeys = Array.range(start, end)
      val intValues = ps.pull(intKeys.clone()).asInstanceOf[IntIntVector].get(intKeys)
      val map = int2long.pull(intKeys ++ intValues).asInstanceOf[IntLongVector]
      map.get(intKeys).zip(map.get(intValues))
    }
  }

  def getRDD: RDD[(Int, Long)] = {
    val sc = SparkContext.getOrCreate()
    val master = PSAgentContext.get().getMasterClient
    val partitions = master.getMatrix(INT2LONG)
      .getPartitionMetas.map { case (_, p) =>
      (p.getStartCol, p.getEndCol)
    }.toSeq
    sc.parallelize(partitions, this.numPSPartition).flatMap { case (start, end) =>
      long2int.pull(Array.range(start.toInt, end.toInt)).asInstanceOf[IntLongVector]
        .getStorage
        .entryIterator()
        .map { entry =>
          (entry.getIntKey, entry.getLongValue)
        }
    }
  }
}

object NodeIndexer {
  val LONG2INT = "long2int"
  val INT2LONG = "int2long"
}
