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
package com.tencent.angel.spark.ml.graph.louvain

import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.utils.NodeIndexer
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

/**
  * Louvain algorithm implementation
  * @param uid
  */
class Louvain(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCommunityIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasBufferSize
  with HasDebugMode {

  final val numOpt = new IntParam(this, "numOpt", "numOpt")
  final val numFold = new IntParam(this, "numFold", "numFold")
  final val eps = new DoubleParam(this, "eps", "eps")

  final def setNumOpt(num: Int): this.type = set(numOpt, num)

  final def setNumFold(num: Int): this.type = set(numFold, num)

  final def setEps(error: Double): this.type = set(eps, error)

  final def getNumOpt: Int = $(numOpt)

  final def getNumFold: Int = $(numFold)

  final def getEps: Double = $(eps)

  setDefault(numOpt, 10)
  setDefault(numFold, 3)
  setDefault(eps, 0.0)

  def this() = this(Identifiable.randomUID("louvain"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    assert(dataset.sparkSession.sparkContext.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    val rawEdges: RDD[((Long, Long), Float)] = {
      if ($(isWeighted)) {
        dataset.rdd.take(2).foreach(println)
        dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd.map { row =>
          (row.getLong(0), row.getLong(1), row.getFloat(2))
        }
      } else {
        dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.map { row =>
          (row.getLong(0), row.getLong(1), 1.0f)
        }
      }
    }.map { case (src, dst, wgt) =>
      if (src < dst) ((src, dst), wgt) else ((dst, src), wgt)
    }.reduceByKey(_ + _, $(partitionNum))
      .persist(StorageLevel.DISK_ONLY)

    val nodes = rawEdges.flatMap { case ((src, dst), _) =>
      Iterator(src, dst)
    }.distinct($(partitionNum))

    val reIndexer = new NodeIndexer()
    reIndexer.train($(psPartitionNum), nodes, $(batchSize))

    //reindex edges from (Long, Long, Float) to (Int, Int, Float)
    val edges: RDD[(Int, Int, Float)] = reIndexer.encode(rawEdges, $(batchSize)) { case (iter, ps) =>
      val keys = iter.flatMap { case ((src, dst), _) => Iterator(src, dst) }.distinct
      val map = ps.pull(keys).asInstanceOf[LongIntVector]
      iter.map { case ((src, dst), wgt) =>
        (map.get(src), map.get(dst), wgt)
      }.toIterator
    }

    val graph: RDD[LouvainGraphPartition] = LouvainGraph.edgeTripleRDD2GraphPartitions(edges,
      storageLevel = $(storageLevel))

    // destroys the lineage and close encoder of node indexer
    graph.checkpoint()
    graph.foreachPartition(_ => Unit)
    reIndexer.destroyEncoder()

    rawEdges.unpersist()

    val model = LouvainPSModel(reIndexer.getNumNodes)
    var louvain = new LouvainGraph(graph, model)
    louvain.updateNodeWeightsToPS()
    louvain.modularityOptimize($(numOpt), $(batchSize), $(eps))

    // correctIds
    var totalSum = louvain.checkTotalSum(model)
    louvain.correctCommunityId(model, $(bufferSize))

    if ($(debugMode)) {
      assert(louvain.checkCommId(model) == 0)
      val total = louvain.checkTotalSum(model)
      assert(total == totalSum, s"$total != $totalSum")
    }


    var foldIter = 0
    while (foldIter < $(numFold)) {
      foldIter += 1
      louvain = louvain.folding($(batchSize), $(storageLevel))
      louvain.modularityOptimize($(numOpt), $(batchSize), $(eps))

      // correctIds
      totalSum = louvain.checkTotalSum(model)
      println(s"total = $totalSum")
      louvain.correctCommunityId(model, $(bufferSize))
      if (foldIter < $(numFold) && $(debugMode)) {
        assert(louvain.checkCommId(model) == 0)
        val total = louvain.checkTotalSum(model)
        assert(total == totalSum, s"$total != $totalSum")
      }
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame({
      reIndexer.decodeInt2IntPSVector(model.node2CommunityPSVector
      ).sortByKey().map { case (id, c) =>
        Row.fromSeq(Seq(id, c))
      }
    }, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCommunityIdCol)}", LongType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
