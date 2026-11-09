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
package com.tencent.angel.spark.ml.graph.algo.embedding.metaPath2vec

import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, IntArrayList}
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.tencent.angel.spark.ml.graph.utils.Stats._


// previous meta-paths are assigned by node types, which are unable to represent meta-paths like
// "writerA writing paperA citing paperB cited by paperC written by writerB", where two edge types exist for 'paper'.
// thus a meta-path assigned by edge types is created, which is "writing->citing->cited->citing->written"

class MetaPath2vecV1(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize
  with HasIsWeighted with HasWeightCol with HasWalkLength with HasNeedReplicaEdge
  with HasPullBatchSize with HasEpochNum with HasEdgeTypeCol with HasDynamicInitNeighbor {

  def this() = this(Identifiable.randomUID("metaPath2vec"))

  private var typeSet: Set[Int] = _
  private var output: String = _
  private var pathMap: Int2ObjectOpenHashMap[Array[Int]] = _

  def setMetaPath(meta: String): Unit = {
    val path_ = meta.trim.split("-").map(_.toInt+1)
    val path = path_.slice(0, path_.length-1)
    typeSet = path.toSet
    val pathMap_ = new Int2ObjectOpenHashMap[Array[Int]](typeSet.size)
    typeSet.foreach { t =>
      val thisPath = new IntArrayList()
      var index = path.indexOf(t) // differs from MetaPath2vec
      var step = 1
      while (step < ${walkLength}) {
        if (index > path.length-1)
          index = 0
        thisPath.add(path(index))
        index += 1
        step += 1
      }
      pathMap_.put(t, thisPath.toIntArray())
    }
    pathMap = pathMap_
  }

  def setOutputDir(in: String): Unit = { output = in }

  def initialize(dataset: Dataset[_]): (MetaPath2VecPSModel, RDD[(Long, Int)]) = {
    val sc = dataset.sparkSession.sparkContext
    var startTime = System.currentTimeMillis()
    val rawEdges = MetaPath2vecV1.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol),
      $(weightCol), $(edgeTypeCol), isWeighted = $(isWeighted))
      .repartition($(partitionNum))
      .persist($(storageLevel))

    val (minId, maxId, numEdges) = rawEdges.map(x => (x._1, x._2)).mapPartitions(summarizeApplyOp).reduce(summarizeReduceOp)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    // start ps and create ps matrix
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val nbrModelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "neighbor", sc.hadoopConfiguration)
    var tagModelContext: ModelContext = null
    val model = new MetaPath2VecPSModel(nbrModelContext, tagModelContext, $(dynamicInitNeighbor))
    model.init()

    // create neighborTable by edge types,
    // do not replicate edges since a->b and b->a may belong to different edge types
    val nodes = if ($(dynamicInitNeighbor)) {
      val rawEdges_ = rawEdges.flatMap(x => Iterator((x._1, x._2, x._3, x._4), (x._2, x._1, x._3, x._4))).persist($(storageLevel))
      val temp = rawEdges_.map(x => ((x._1, x._3), 1)).reduceByKey(_ + _)
      temp.persist($(storageLevel))

      val degree = temp
        .map(x => (x._1._1, (x._1._2, x._2)))
        .groupByKey()
        .map(x => (x._1, x._2.toArray))
      degree.persist($(storageLevel))

      MetaPath2VecOperator.initNeighborTableWithEdgeType_(rawEdges_, degree, $(batchSize), model, $(isWeighted))

      val nodes = temp.map(x => (x._1._1, x._1._2))
      nodes.persist($(storageLevel))
      temp.unpersist()
      degree.unpersist()
      rawEdges_.unpersist()
      nodes

    } else {
      val neighborTable = rawEdges.flatMap(x => Iterator((x._1, (x._2, x._3, x._4)), (x._2, (x._1, x._3, x._4))))
        .groupByKey($(partitionNum))
        .map(x => (x._1, x._2.toArray.distinct))
      //    neighborTable.unpersist()

      neighborTable.foreachPartition { iter => MetaPath2VecOperator.initNeighborTableWithEdgeType(iter, $(batchSize), model, $(isWeighted)) }
      val before = System.currentTimeMillis()
      model.checkpoint()
      println(s"checkpoint cost ${System.currentTimeMillis() - before} ms.")

      // start sampling
      val nodes = neighborTable.map(_._1).mapPartitions { iter => model.getNodeEdgeTypes(iter.toArray) }
      nodes.persist($(storageLevel))
      println(s"num starting points: ${nodes.count()}")
      neighborTable.unpersist()
      nodes
    }
    rawEdges.unpersist()
    println(s"initialize, cost ${System.currentTimeMillis() - startTime} ms.")
    (model, nodes)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val (model, nodes) = initialize(dataset)

    val conf = SparkContext.getOrCreate().getConf
    val savePartition = conf.getInt("spark.hadoop.angel.metapath.save.partition", $(partitionNum))
    var epoch = 0
    while (epoch < ${epochNum}) {
      val startTime = System.currentTimeMillis()
      for (thisType <- typeSet) {
        val thisPath = pathMap.get(thisType)
        val startNodes = nodes.filter(_._2 == thisType)
        val thisResult = startNodes.mapPartitionsWithIndex { case (index, iter) =>
          MetaPath2VecOperator.sample(index, iter, thisPath, model, $(pullBatchSize), dynamicInitNeighbor=$(dynamicInitNeighbor))
        }
        // write
        GraphIO.appendSave(dataset.sparkSession.createDataFrame(thisResult.coalesce(savePartition), transformSchema(dataset.schema)), output)
      }
      println(s"finished $epoch-th iteration, cost ${System.currentTimeMillis()-startTime} ms.")
      epoch += 1
    }

    val t = SparkContext.getOrCreate().parallelize(List("1", "2"), 1)
    dataset.sparkSession.createDataFrame(t.map(x => Row(x)), transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("path", StringType, nullable = false)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object MetaPath2vecV1 {

  // must have edge type
  def loadEdges(dataset: Dataset[_], srcNodeIdCol: String, dstNodeIdCol: String, weightCol: String, edgeTypeCol: String,
                isWeighted: Boolean = false): RDD[(Long, Long, Int, Float)] = {
    // add 1 to types to match the meta-path
    if (isWeighted) {
      dataset.select(srcNodeIdCol, dstNodeIdCol, edgeTypeCol, weightCol).rdd
        .filter(!_.anyNull)
        .mapPartitions { iter =>
          iter.flatMap { row =>
            if (row.getLong(0) == row.getLong(1) || row.getFloat(3) <= 0) Iterator.empty
            else Iterator.single(row.getLong(0), row.getLong(1), row.getLong(2).toInt+1, row.getFloat(3))
          }
        }
    } else {
      dataset.select(srcNodeIdCol, dstNodeIdCol, edgeTypeCol).rdd
        .filter(!_.anyNull)
        .mapPartitions { iter =>
          iter.flatMap { row =>
            if (row.getLong(0) == row.getLong(1)) Iterator.empty
            else Iterator.single(row.getLong(0), row.getLong(1), row.getLong(2).toInt+1, 1.0f)
          }
        }
    }
  }
}

