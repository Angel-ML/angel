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
package com.tencent.angel.spark.ml.graph.algo.rank.multiAttrAggregate

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex.MultiAttrHIndexPSModel
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

// Aggregation by node's attributes: sum/mean/max/min
// There may exist more than one attribute
// The attributes are of type float

class BiGraphAggre(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol with HasPullBatchSize
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition with HasBalancePartitionPercent
  with HasNodeAttrPath with HasNodeAttrNum {

  def this() = this(Identifiable.randomUID("Aggregate"))

  var nodeAttrs: RDD[(Long, Array[Float])] = _

  var output: String = _

  def setOutput(in: String): Unit = {
    this.output = in
  }

  def setNodeAttr(nodeAttr: RDD[(Long, Array[Float])]): Unit = {
    this.nodeAttrs = nodeAttr
  }

  // output: two columns: node and aggregateResults,
  // aggregateResult is a string separated by space, every four values represents sum/mean/max/min for one attribute
  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges
    val edges = if ($(isWeighted)) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .map(row => (row.getLong(0), (row.getLong(1), row.getFloat(2))))
        .filter(f => f._2._2 != 0)
    } else {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .map(row => (row.getLong(0), (row.getLong(1), 1.0f)))
    }
    edges.persist(StorageLevel.DISK_ONLY)

    val numEdges = edges.count()
    val nodes = edges.flatMap(e => Iterator(e._1, e._2._1))
    val maxId = nodes.max()
    val minId = nodes.min()

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "attr", SparkContext.getOrCreate().hadoopConfiguration)
    val model = MultiAttrHIndexPSModel.fromMinMax($(batchSize), modelContext , nodes,  $(useBalancePartition))

    var num = model.initAttributes(nodeAttrs)
    println(s"Init $num users' attrs.")
    model.checkpoint()
    println(s"checkpoint finished.")

    // step1, aggregate users' features to item nodes
    val graph0 = edges.map(x => (x._2._1, (x._1, x._2._2))).groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(MultiAttrAggregateGraphPartition.apply(index, adjTable)))
    graph0.persist($(storageLevel))
    graph0.foreachPartition(_ => Unit)

    val result0 = graph0.flatMap(_.process(model, $(pullBatchSize), $(nodeAttrNum))).persist($(storageLevel))
    result0.count()
    num = model.initAttributes(result0)
    println(s"calculated and put $num items' attrs (aggregated from users) to ps.")
    model.checkpoint(1)
    println(s"checkpoint finished.")

    // step2, aggregate items'features to user nodes
    graph0.unpersist()
    val graph = edges.groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(MultiAttrAggregateGraphPartition.apply(index, adjTable)))
    val result = graph.flatMap(_.process1(model, $(pullBatchSize), $(nodeAttrNum) * 4)).persist($(storageLevel))

    dataset.sparkSession.createDataFrame(result.map(x => Row(x._1, x._2.mkString(" "))), transformSchema(dataset.schema))

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        "nodeAttrAggregate"
      }", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}