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
package com.tencent.angel.graph.embedding.metapath2vec

import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, IntArrayList}
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.graph.utils.GraphIO
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.tencent.angel.graph.utils.Stats._


class MetaPath2Vec(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize
  with HasIsWeighted with HasWeightCol with HasWalkLength with HasNeedReplicaEdge
  with HasPullBatchSize with HasEpochNum {

  def this() = this(Identifiable.randomUID("metaPath2vec"))

  private var typeSet: Set[Int] = _
  private var nodeAttr: RDD[(Long, Int)] = _
  private var output: String = _
  private var pathMap: Int2ObjectOpenHashMap[Array[Int]] = _

  def setMetaPath(meta: String): Unit = {
    val path_ = meta.trim.split("-").map(_.toInt+1)
    val path = path_.slice(0, path_.length-1)
    typeSet = path.toSet
    val pathMap_ = new Int2ObjectOpenHashMap[Array[Int]](typeSet.size)
    typeSet.foreach { t =>
      val thisPath = new IntArrayList()
      var index = path.indexOf(t) + 1
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
  def setNodeAttr(attr: RDD[(Long, Int)]): Unit = { nodeAttr = attr }
  def setOutputDir(in: String): Unit = { output = in }


  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext

    val rawEdges = NeighborDataOps.loadEdgesWithWeight(dataset, $(srcNodeIdCol), $(dstNodeIdCol),
      $(weightCol), isWeighted = $(isWeighted))
      .repartition($(partitionNum))
      .persist($(storageLevel))

    val (minId, maxId, numEdges) = rawEdges.mapPartitions(summarizeApplyOp1).reduce(summarizeReduceOp)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    // start ps and create ps matrix
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val nbrModelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "neighbor", sc.hadoopConfiguration)
    var tagModelContext: ModelContext = null
    if (nodeAttr != null)
      tagModelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
        "tag", sc.hadoopConfiguration)
    val model = new MetaPath2VecPSModel(nbrModelContext, tagModelContext)
    model.init()

    // push node attr to ps
    if (nodeAttr != null) {
      val num = nodeAttr.mapPartitions { iter => model.initMsgs(iter.toArray, $(batchSize)) }
        .reduce(_ + _)
      println(s"init $num node with type. nnz on ps: ${model.numMsgs()}")
    }

    // pull node type from ps and join with edges
    val neighborTable = rawEdges.flatMap(x => Iterator((x._1, (x._2, x._3)), (x._2, (x._1, x._3))))
      .groupByKey($(partitionNum))
      .map(x => (x._1, x._2.toArray.distinct))
    //    neighborTable.unpersist()

    if (nodeAttr != null) {
      neighborTable.foreachPartition{ iter => MetaPath2VecOperator.initNeighborTableWithType(iter, $(batchSize), model, $(isWeighted)) }
    } else {
      neighborTable.foreachPartition{ iter => MetaPath2VecOperator.initNeighborTable(iter, $(batchSize), model, $(isWeighted)) }
    }
    val before = System.currentTimeMillis()
    model.checkpoint()
    println(s"checkpoint cost ${System.currentTimeMillis()-before} ms.")

    // start sampling
    val nodes = if (nodeAttr != null)
      neighborTable.map(_._1).mapPartitions { iter => model.getTypes(iter.toArray, typeSet)}
    else neighborTable.map(x => (x._1, 1))
    nodes.persist($(storageLevel))
    println(s"num nodes: ${nodes.count()}")
    neighborTable.unpersist()
    rawEdges.unpersist()

    var epoch = 0
    while (epoch < ${epochNum}) {
      val startTime = System.currentTimeMillis()
      for (thisType <- typeSet) {
        val thisPath = pathMap.get(thisType)
        val startNodes = nodes.filter(_._2 == thisType)
        val thisResult = startNodes.mapPartitionsWithIndex { case (index, iter) =>
          MetaPath2VecOperator.sample(index, iter, thisPath, model, $(pullBatchSize))
        }
        // write
        GraphIO.appendSave(dataset.sparkSession.createDataFrame(thisResult, transformSchema(dataset.schema)), output)
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
