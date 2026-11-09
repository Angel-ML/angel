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
package com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

// Use node's attributes (instead of node's degree) to calculate h-index
// There may exist more than one attribute
// The attributes is of type float
// Since degree and attribute may have different range of values, maybe a normalization method is needed.

class MultiAttrHIndex(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasNodeAttrPath with HasMaxIteration {

  def this() = this(Identifiable.randomUID("H-Index"))

  var nodeAttrs: RDD[(Long, Array[Float])] = _

  def setNodeAttr(nodeAttr: RDD[(Long, Array[Float])]): Unit = {
    this.nodeAttrs = nodeAttr
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    //edges's storageLevel choose  Disk_Only
    edges.persist(StorageLevel.DISK_ONLY)

    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val maxId = edges.map(e => math.max(e._1, e._2)).max() + 1
    val minId = edges.map(e => math.min(e._1, e._2)).min()
    val numEdges = edges.count()

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    //     Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "attr", SparkContext.getOrCreate().hadoopConfiguration)
    val model = MultiAttrHIndexPSModel.fromMinMax($(batchSize), modelContext, nodes, $(useBalancePartition))

    val numNodesWithAttrs = nodeAttrs.count()
    println(s"numNodes with attrs=$numNodesWithAttrs")

    val num = model.initAttributes(nodeAttrs)
    println(s"Init $num nodes attrs.")

    val graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(MultiAttrHIndexGraphPartition.apply(index, adjTable)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.persist($(storageLevel))
    var result = graph.map(_.process(model, $(batchSize)))
    result.count()
    var iter = 1
    while (iter < $(maxIteration)) {
      // update attrs in ps
      model.initAttributes(result.flatMap{ case (nodes, values) => nodes.zip(values.map(x => x.map(_.toFloat)))})
      // next iteration
      result = graph.map(_.process(model, $(batchSize)))
      result.count()
      iter += 1
    }
    //    val res =graph.map(_.save()).cache()

    val retRDD = result.flatMap{ case (nodes, cores) => nodes.zip(cores)}
      .map(r => Row.fromSeq(Seq[Any](r._1, r._2.mkString(" "))))
    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        "nodeAttrHIndex"
      }", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}