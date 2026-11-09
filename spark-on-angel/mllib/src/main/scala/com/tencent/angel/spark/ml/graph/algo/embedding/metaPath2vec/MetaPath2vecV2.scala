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

import it.unimi.dsi.fastutil.ints.IntArrayList
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
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap


// random walk by method in GATNE

class MetaPath2vecV2(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize
  with HasIsWeighted with HasWeightCol with HasWalkLength with HasNeedReplicaEdge
  with HasPullBatchSize with HasEpochNum with HasEdgeTypeCol with HasDynamicInitNeighbor{

  def this() = this(Identifiable.randomUID("metaPath2vec"))

  private var output: String = _
  private var pathMap: Array[(Long, Int2ObjectOpenHashMap[Array[Int]])] = _
  private var nodeAttr: RDD[(Long, Int)] = _

  def setMetaPathInfo(info: String): Unit = {
    // info eg. "edgeType0:metaPath0,metaPath1 edgeType1:metaPath0,metaPath1 ..."
//    val temp = info.split(" ").map(_.split(":")).map(x => (x(0), x(1).split(",")))
    val temp = info.split(" ").map(_.split(":")).map { x =>
      val edgeType = x(0)
      val metas = if (x.length == 1) Array[String]() else x(1).split(",")
      (edgeType, metas)
    }
    val pathMap = temp.map { case (edgeType, metas) =>
      val pathMap_ = new Int2ObjectOpenHashMap[Array[Int]](metas.length)
      metas.foreach { meta =>
        val path_ = meta.trim.split("-").map(_.toInt+1)
        val path = path_.slice(0, path_.length-1)
        val thisPath = new IntArrayList()
        var index = 1
        var step = 1
        while (step < ${walkLength}) {
          if (index > path.length-1)
            index = 0
          thisPath.add(path(index))
          index += 1
          step += 1
        }
        pathMap_.put(path_(0), thisPath.toIntArray())
      }
      (edgeType.toLong, pathMap_)
    }
    this.pathMap = pathMap
  }

  def printPathInfo(): Unit = {
    println(s"----- metaPaths -----")
    val temp = pathMap.map{ case (edgeType, paths) => edgeType + ":" + paths.keySet().toIntArray.map(x => paths.get(x).mkString("-")).mkString(",") }
    temp.foreach(println)
    println(s"---------------------")
  }

  def setOutputDir(in: String): Unit = { output = in }
  def setNodeAttr(attr: RDD[(Long, Int)]): Unit = { nodeAttr = attr }


  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    val conf = SparkContext.getOrCreate().getConf
    val savePartition = conf.getInt("spark.hadoop.angel.metapath.save.partition", $(partitionNum))
    val rawEdges = MetaPath2vecV2.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol),
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
    if (nodeAttr != null)
      tagModelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
        "tag", sc.hadoopConfiguration)
    val model = new MetaPath2VecPSModel(nbrModelContext, tagModelContext, $(dynamicInitNeighbor))
    model.init()

    // push node attr to ps
    if (nodeAttr != null) {
      val num = nodeAttr.mapPartitions { iter => model.initMsgs(iter.toArray, $(batchSize)) }
        .reduce(_ + _)
      println(s"init $num node with type. nnz on ps: ${model.numMsgs()}")
    }

    // process by edgeTypes
    this.pathMap.foreach { case (edgeType, paths) =>
      println(s"start random walking by edgeType $edgeType")
      var startTime = System.currentTimeMillis()
      val rawEdges_ = rawEdges.filter(_._3 == edgeType)
        .flatMap(x => Iterator((x._1, x._2, x._4), (x._2, x._1, x._4)))
        .persist($(storageLevel))

      val nodes = if ($(dynamicInitNeighbor)) {
        if (nodeAttr != null) {
          val degree = rawEdges_.map(r => (r._2, r._1))     // (dst, stc)
            .join(nodeAttr)                                 // (dst, (src, dst_type))
            .map(x => ((x._2._1, x._2._2), 1))              // ((src, dst_type), 1)
            .reduceByKey(_ + _)                             // ((src, dst_type), dst_count)
            .map(x => (x._1._1, (x._1._2, x._2)))           // (src, (dst_type, dst_count))
            .groupByKey()                                   // (src, Iterator(dst_type, dst_count))
            .map(x => (x._1, x._2.toArray))                 // (src, Array(dst_type, dst_count))
          degree.persist($(storageLevel))

          MetaPath2VecOperator.initNeighborTableWithType_(rawEdges_, degree, $(batchSize), model, $(isWeighted))

          val nodes = if (paths.isEmpty) degree.map(_._1).map(x => (x, 0))
          else degree.map(_._1).mapPartitions { iter => model.getTypes(iter.toArray) }
            .persist($(storageLevel))
          println(s"num nodes: ${nodes.count()}")
          degree.unpersist()
          nodes
        } else {
          val degree = rawEdges_.map(x => (x._1, 1)).reduceByKey(_ + _)
          degree.persist($(storageLevel))

          MetaPath2VecOperator.initNeighborTable_(rawEdges_, degree, $(batchSize), model, $(isWeighted))

          val nodes = degree.map(x => (x._1, 1))
          nodes.persist($(storageLevel))
          degree.unpersist()
          nodes
        }

      } else {
        val neighborTable = rawEdges_.map(x => (x._1, (x._2, x._3)))
          .groupByKey($(partitionNum))
          .map(x => (x._1, x._2.toArray.distinct))

        if (nodeAttr != null) {
          neighborTable.foreachPartition { iter => MetaPath2VecOperator.initNeighborTableWithType(iter, $(batchSize), model, $(isWeighted)) }
        } else {
          neighborTable.foreachPartition { iter => MetaPath2VecOperator.initNeighborTable(iter, $(batchSize), model, $(isWeighted)) }
        }
        val before = System.currentTimeMillis()
        model.checkpoint()
        println(s"checkpoint cost ${System.currentTimeMillis() - before} ms.")

        val nodes = if (paths.isEmpty) neighborTable.map(_._1).map(x => (x, 0))
        else neighborTable.map(_._1).mapPartitions { iter => model.getTypes(iter.toArray) }
          .persist($(storageLevel))
        println(s"num nodes: ${nodes.count()}")
        neighborTable.unpersist()
        rawEdges_.unpersist()
        nodes
      }
      println(s"initialize edgeType $edgeType, cost ${System.currentTimeMillis() - startTime} ms.")

      var epoch = 0
      while (epoch < $(epochNum)) {
        val startTime = System.currentTimeMillis()
        if (paths.isEmpty) {
          val thisPath = new Array[Int]($(walkLength) - 1)
          val thisResult = nodes.mapPartitionsWithIndex { case (index, iter) =>
            MetaPath2VecOperator.sample(index, iter, thisPath, model, $(pullBatchSize), sampleByType = false, dynamicInitNeighbor=$(dynamicInitNeighbor))
          }.map(x => Row(s"$edgeType " + x.getString(0)))
          GraphIO.appendSave(dataset.sparkSession.createDataFrame(thisResult.coalesce(savePartition), transformSchema(dataset.schema)), output)
        } else {
          val types = paths.keySet().toIntArray
          types.foreach { thisType =>
            val thisPath = paths.get(thisType)
            val startNodes = nodes.filter(_._2 == thisType)
            val thisResult = startNodes.mapPartitionsWithIndex { case (index, iter) =>
              MetaPath2VecOperator.sample(index, iter, thisPath, model, $(pullBatchSize), dynamicInitNeighbor=$(dynamicInitNeighbor))
            }.map(x => Row(s"$edgeType " + x.getString(0)))
            GraphIO.appendSave(dataset.sparkSession.createDataFrame(thisResult.coalesce(savePartition), transformSchema(dataset.schema)), output)
          }
        }
        println(s"finished $epoch-th iteration, cost ${System.currentTimeMillis()-startTime} ms.")
        epoch += 1
      }
        model.clearNeighborTable()
    }

    val t = SparkContext.getOrCreate().parallelize(List("1", "2"), 1)
    dataset.sparkSession.createDataFrame(t.map(x => Row(x)), transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("path", StringType, nullable = false)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object MetaPath2vecV2 {

  // must have edge type
  def loadEdges(dataset: Dataset[_], srcNodeIdCol: String, dstNodeIdCol: String, weightCol: String, edgeTypeCol: String,
                isWeighted: Boolean = false): RDD[(Long, Long, Long, Float)] = {
    // add 1 to types to match the meta-path
    if (isWeighted) {
      dataset.select(srcNodeIdCol, dstNodeIdCol, edgeTypeCol, weightCol).rdd
        .filter(!_.anyNull)
        .mapPartitions { iter =>
          iter.flatMap { row =>
            if (row.getLong(0) == row.getLong(1) || row.getFloat(3) <= 0) Iterator.empty
            else Iterator.single(row.getLong(0), row.getLong(1), row.getLong(2), row.getFloat(3))
          }
        }
    } else {
      dataset.select(srcNodeIdCol, dstNodeIdCol, edgeTypeCol).rdd
        .filter(!_.anyNull)
        .mapPartitions { iter =>
          iter.flatMap { row =>
            if (row.getLong(0) == row.getLong(1)) Iterator.empty
            else Iterator.single(row.getLong(0), row.getLong(1), row.getLong(2), 1.0f)
          }
        }
    }
  }
}

