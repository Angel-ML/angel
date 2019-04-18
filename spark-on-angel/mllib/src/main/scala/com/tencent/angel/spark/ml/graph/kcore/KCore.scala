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

package com.tencent.angel.spark.ml.graph.kcore

import com.tencent.angel.ml.math2.vector.{IntLongVector, LongIntVector}
import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.utils.NodeIndexer
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


class KCore(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize {

  def this() = this(Identifiable.randomUID("KCore"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val rawEdges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.flatMap { row =>
      val src = row.getLong(0)
      val dst = row.getLong(1)
      if (src != dst) {
        Iterator.single((src, dst))
      } else {
        Iterator.empty
      }
    }.persist(StorageLevel.MEMORY_ONLY)

    val nodes = rawEdges.mapPartitions { iter =>
      val distinct = collection.mutable.HashSet[Long]()
      iter.foreach { case (src, dst) =>
        distinct.add(src)
        distinct.add(dst)
      }
      distinct.toIterator
    }.distinct($(partitionNum))


    val indexer = new NodeIndexer()
    indexer.train($(psPartitionNum), nodes)

    val edges: RDD[(Int, Int)] = indexer.encode(rawEdges, $(batchSize)) { case (arr, ps) =>
      val keys = arr.flatMap(t => Array(t._1, t._2)).distinct
      val map = ps.pull(keys.clone()).asInstanceOf[LongIntVector]
      arr.flatMap { case (src, dst) =>
        val intSrc = map.get(src)
        val intDst = map.get(dst)
        Iterator((intSrc, intDst), (intDst, intSrc))
      }(collection.breakOut)
    }

    val graph = edges.groupByKey($(partitionNum)).mapPartitions { iter =>
      val keys = new ArrayBuffer[Int]()
      val values = new ArrayBuffer[Array[Int]]()
      iter.foreach { case (key, group) =>
        keys += key
        values += group.toSet.toArray
      }
      Iterator.single(KCoreGraphPartition(keys.toArray, values.toArray))
    }.persist($(storageLevel))

    graph.checkpoint()
    graph.foreachPartition(_ => Unit)
    rawEdges.unpersist(false)
    indexer.destroyEncoder()

    val numNodes = indexer.getNumNodes
    val model = KCorePSModel.fromMaxId(numNodes)

    // init
    graph.foreach(_.init(model))
    println(s"init core sum: ${graph.map(_.sum(model)).sum()}")

    var numMsg = Long.MaxValue
    var iterNum = 0
    var version = 0

    while (numMsg > 0) {
      iterNum += 1
      version += 1
      numMsg = graph.map(_.process(model, version, numMsg < numNodes * 0.1)).reduce(_ + _)
      println(s"iter-$iterNum, num node updated: $numMsg")

      // reset version
      if (Coder.isMaxVersion(version + 1)) {
        println("reset version")
        version = 0
        graph.foreach(_.resetVersion(model))
      }

      // show sum of cores every 10 iter
      if (iterNum % 10 == 0) {
        val sum = graph.map(_.sum(model)).sum()
        println(s"iter-$iterNum, core sum = $sum")
      }
    }

    println(s"iteration end in $iterNum round, final core sum is ${graph.map(_.sum(model)).sum()}")

    val retRDD = graph.map(_.save(model))

    // decode
    val result = indexer.decodePartition[(Array[Int], Array[Int]), (Long, Int)](retRDD) { ps =>
      iter =>
        if (iter.nonEmpty) {
          val (node, rank) = iter.next()
          val intKeys = ps.pull(node.clone()).asInstanceOf[IntLongVector].get(node)
          intKeys.zip(rank).toIterator
        } else {
          Iterator.empty
        }
    }.map { case (node, rank) =>
      Row.fromSeq(Seq[Any](node, rank))
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(result, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCoreIdCol)}", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
