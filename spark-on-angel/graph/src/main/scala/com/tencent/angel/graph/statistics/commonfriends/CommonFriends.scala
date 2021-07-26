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

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.model.neighbor.simple.SimpleNeighborTableModel
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

  def this() = this(Identifiable.randomUID("commonFriends"))

  private var maxComFriendsNum: Int = Int.MaxValue
  def setMaxComFriendsNum(in: Int): Unit = { this.maxComFriendsNum = in }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sc = dataset.sparkSession.sparkContext

    val numPart = sc.getConf.getInt("spark.default.parallelism", $(partitionNum))
    println(s"default parallelism: $numPart")
    println(s"partition number: ${$(partitionNum)}")

    println(s"======load edges from the first input======")
    val firstEdges: RDD[(Long, Long)] = {
      if ($(isCompressed)) {
        NeighborDataOps.loadCompressedEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(compressCol))
      } else {
        NeighborDataOps.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol))
      }
    }

    println(s"======sample edges======")
    println(firstEdges.take(10).mkString(","))

    println(s"======convert edges to neigbors tables======")
    val firstNeighbors: RDD[(Long, Array[Long])] =
      NeighborDataOps.edges2NeighborTable(firstEdges, $(partitionNum)).persist($(storageLevel))

    println(s"======sample neighbor tables======")
    firstNeighbors.take(10).foreach { case (src, neighbors) =>
      println(s"src = $src, neighbors = ${neighbors.mkString(",")}")
    }

    println(s"======statistics of the data======")
    val stats = NeighborDataOps.statsByNeighborTable(firstNeighbors)
    println(s"min node id = ${stats._1}")
    println(s"max node id = ${stats._2}")
    println(s"num of nodes = ${stats._3}")
    println(s"num of edges = ${stats._4}")

    println(s"======start parameter server======")
    val psStartTime = System.currentTimeMillis()
    startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"======push neighbor tables to parameter server======")
    val initTableStartTime = System.currentTimeMillis()
    val modelContext = new ModelContext($(psPartitionNum), stats._1, stats._2 + 1, stats._3,
      "simple_neighbor", sc.hadoopConfiguration)
    val model = new SimpleNeighborTableModel(modelContext)
    model.init()

    firstNeighbors.mapPartitions { iter => {
      // Init the neighbor table use many mini-batch to avoid big object
      iter.sliding($(batchSize), $(batchSize)).map(pairs => model.initNeighbors(pairs))
    }
    }.count()

    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    model.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    NeighborDataOps.testPS(firstNeighbors, model, 10)
    val checkValid = NeighborDataOps.checkValid(firstNeighbors, model, 10)
    require(checkValid, s"result with executor RDD and that with PS are different")

    println(s"======load edges from the second input======")
    val extraInput = $(extraInputs)
    assert(extraInput.length == 1, s"multiple inputs for non-friends are not supported")
    val isOneInput = extraInput(0).equals($(input))

    if (!isOneInput) firstNeighbors.unpersist()

    val secondDF: DataFrame = if (!isOneInput) {
      GraphIO.load(extraInput(0),
        isWeighted = false,
        srcIndex = $(srcNodeIndex),
        dstIndex = $(dstNodeIndex),
        sep = $(delimiter))
    } else null

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
    println(s"num of edges in the second input = $numSecondEdges")

    println(s"======sample edges in the second input======")
    println(secondEdges.take(10).mkString(","))

    println(s"======start calculation======")
    val rawResult: RDD[Row] = if (!isOneInput) {
      secondEdges.mapPartitionsWithIndex { case (partId, iter) =>
        CommonFriendsOperator.runEdgePartition(iter, partId, ${pullBatchSize}, model, maxComFriendsNum)
      }
    } else {
      firstNeighbors.mapPartitionsWithIndex { case (partId, iter) =>
        CommonFriendsOperator.runNeighborPartition(iter, partId, ${pullBatchSize}, model, maxComFriendsNum)
      }
    }

    println(s"======sample results======")
    rawResult.take(10).foreach { row =>
      println(s"src = ${row.getLong(0)}, dst = ${row.getLong(1)}, num of common friends = ${row.getInt(2)}")
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rawResult, outputSchema)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(dstNodeIdCol), LongType, nullable = false),
      StructField($(numCommonFriendsCol), IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}