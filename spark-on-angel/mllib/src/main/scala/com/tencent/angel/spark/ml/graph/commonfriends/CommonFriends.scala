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

import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.utils.NodeIndexer
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

class CommonFriends(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasBufferSize
  with HasDebugMode {

  def this() = this(Identifiable.randomUID("commonfriends"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    assert(dataset.sparkSession.sparkContext.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    val rawEdges: RDD[(Long, Long)] = {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.map { row =>
        (row.getLong(0), row.getLong(1))
      }
    }.persist(StorageLevel.DISK_ONLY)

    val nodes = rawEdges.flatMap { case (src, dst) =>
      Iterator(src, dst)
    }.distinct($(partitionNum))

    val reIndexer = new NodeIndexer()
    reIndexer.train($(psPartitionNum), nodes)

    val edges: RDD[(Int, Int)] = reIndexer.encode(rawEdges, 1000000) { case (iter, ps) =>
      val keys = iter.flatMap { case (src, dst) => Iterator(src, dst) }.distinct
      val map = ps.pull(keys).asInstanceOf[LongIntVector]
      iter.map { case (src, dst) =>
        (map.get(src), map.get(dst))
      }.toIterator
    }

    val graph: RDD[CommonFriendsPartition] = CommonFriendsGraph.edgeTupleRDD2GraphPartitions(edges,
      storageLevel = $(storageLevel))

    // destroys the lineage and close encoder of node indexer
    graph.checkpoint()
    graph.foreachPartition(_ => Unit)
    reIndexer.destroyEncoder()

    rawEdges.unpersist()

    val outputSchema = transformSchema(dataset.schema)
    dataset.toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object CommonFriends {

}
