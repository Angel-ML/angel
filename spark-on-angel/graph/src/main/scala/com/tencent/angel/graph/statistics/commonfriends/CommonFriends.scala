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

package com.tencent.angel.graph.statistics.commonfriends

import com.tencent.angel.graph.utils.io.Log
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.graph.utils.{GraphIO, PartitionTools}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

class CommonFriends(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasCompressCol
  with HasIsCompressed with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasCompressIndex
  with HasInput with HasExtraInputs with HasDelimiter {

  def this() = this(Identifiable.randomUID("CommonFriends"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")

    Log.withTimePrintln(s"======load edges from the first input======")
    val firstEdges: RDD[(Long, Long)] = {
      if ($(isCompressed))
        CommonFriendsOperator.loadCompressedEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(compressCol))
      else
        CommonFriendsOperator.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol))
    }

    Log.withTimePrintln(s"======convert edges to neigbor tables======")
    val firstNeighbors: RDD[(Long, Array[Long])] =
      CommonFriendsOperator.edges2NeighborTable(firstEdges, $(partitionNum)).persist($(storageLevel))

    Log.withTimePrintln(s"======statistics of the data======")
    val stats = CommonFriendsOperator.statsByNeighborTable(firstNeighbors)
    Log.withTimePrintln(s"num of nodes = ${stats._3}")
    Log.withTimePrintln(s"num of edges = ${stats._4}")
    Log.withTimePrintln(s"min node id = ${stats._1}, max node id = ${stats._2}")

    Log.withTimePrintln(s"======start parameter server======")
    val psStartTime = System.currentTimeMillis()
    startPS(dataset.sparkSession.sparkContext)

    Log.withTimePrintln(s"======push neighbor tables to parameter server======")
    val psModel = CommonFriendsPSModel(stats._2 + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))
    psModel.initLongNeighborTable(firstNeighbors)
    psModel.checkpoint()

    CommonFriendsOperator.testPS(firstNeighbors, psModel, 10)
    val checkValid = CommonFriendsOperator.checkValid(firstNeighbors, psModel, 10)
    require(checkValid, s"result with executor RDD and that with PS are different")

    Log.withTimePrintln(s"======load edges from the second input======")
    val extraInput = $(extraInputs)
    assert(extraInput.length == 1, s"multiple inputs for non-friends are not supported")
    val isOneInput = extraInput(0).equals($(input))

    val secondDF: DataFrame = if (!isOneInput) {
      firstNeighbors.unpersist()
      GraphIO.load(extraInput(0),
        isWeighted = false,
        srcIndex = $(srcNodeIndex),
        dstIndex = $(dstNodeIndex),
        sep = $(delimiter))
    } else {
      null
    }

    //use 2D partition strategy to balance vertices
    val partitioner = PartitionTools.edge2DPartitioner($(partitionNum))
    val secondEdges: RDD[(Long, Long)] = if (!isOneInput) {
      secondDF.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1))
            Iterator.empty
          else
            Iterator.single((row.getLong(0), row.getLong(1)), 0)
        }
      }.repartitionAndSortWithinPartitions(partitioner)
        .map(_._1)
        .persist($(storageLevel))
    } else sc.emptyRDD

    val numSecondEdges = secondEdges.count()
    Log.withTimePrintln(s"num of edges in the second input = $numSecondEdges")

    Log.withTimePrintln(s"======start calculation======")
    val rawResult: RDD[Row] = if (!isOneInput) {
      secondEdges.mapPartitionsWithIndex { case (partId, iter) =>
        CommonFriendsOperator.runEdgePartition(iter, partId, psModel)
      }
    } else {
      firstNeighbors.mapPartitionsWithIndex { case (partId, iter) =>
        CommonFriendsOperator.runNeighborPartition(iter, partId, psModel)
      }
    }

    Log.withTimePrintln(s"======sample results======")
    rawResult.take(10).foreach { row =>
      Log.withTimePrintln(s"src = ${row.getLong(0)}, dst = ${row.getLong(1)}, num of common friends = ${row.getInt(2)}")
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rawResult, outputSchema)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"$srcNodeIdCol", LongType, nullable = false),
      StructField(s"$dstNodeIdCol", LongType, nullable = false),
      StructField(s"$numCommonFriendsCol", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}