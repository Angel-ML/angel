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
import com.tencent.angel.spark.ml.graph.utils.GraphIO
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
// The attributes is of type float

class MultiAttrAggregate(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol with HasPullBatchSize
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasExtraInputs with HasMaxIteration with HasNodeAttrNum {

  def this() = this(Identifiable.randomUID("Aggregate"))

  var nodeAttrs: RDD[(Long, Array[Float])] = _

  var output: String = _

  def setOutput(in: String): Unit = {
    this.output = in
  }

  // output: two columns: node and aggregateResults,
  // aggregateResult is a string separated by space, every four values represents sum/mean/max/min for one attribute
  override def transform(dataset: Dataset[_]): DataFrame = {

    val attrFiles = $(extraInputs)
    assert(attrFiles.nonEmpty, s"attrFiles is empty.")

    //read edges
    val edges = if ($(isWeighted)) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2)
        .filter(f => f._3 != 0)
    } else {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(f => f._1 != f._2)
    }
    edges.persist(StorageLevel.DISK_ONLY)

    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val maxId = nodes.max() + 1
    val minId = nodes.min()
    val numEdges = edges.count()

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    // qq-group graph
    var graph = edges.flatMap { case (srcId, dstId, weight) => Iterator((srcId, (dstId, weight)), (dstId, (srcId, weight))) }
        .groupByKey($(partitionNum))
        .mapPartitionsWithIndex((index, adjTable) => Iterator(MultiAttrAggregateGraphPartition.apply(index, adjTable)))

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "attr", SparkContext.getOrCreate().hadoopConfiguration)
    val model = MultiAttrHIndexPSModel.fromMinMax($(batchSize), modelContext, nodes, $(useBalancePartition))

    attrFiles.foreach { attrFile =>
      nodeAttrs = MultiAttrAggregate.readAttrs(attrFile, 0, 1, "\t")
      val num = model.initAttributes(nodeAttrs)
      println(s"Init $num nodes attrs.")
    }

    model.checkpoint()
    println(s"checkpoint finished.")

    nodeAttrs.unpersist()
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.persist($(storageLevel))

    var iter = 0
    val result = graph.flatMap(_.process(model, $(pullBatchSize), $(nodeAttrNum))).persist($(storageLevel))
    result.count()
    val tempRe = dataset.sparkSession.createDataFrame(result.map(r => Row(r._1, r._2.mkString(" "))), transformSchema(dataset.schema))
    val tempOutputDir = output + s"/$iter"
    GraphIO.save(tempRe, tempOutputDir)
    println(s"saved iter$iter result to $tempOutputDir")
    iter += 1

    if (iter < $(maxIteration)) {
//      model.reset()
      val num = model.initAttributes(result)
      println(s"iter$iter, init $num attrs to ps")
    }

    while (iter < $(maxIteration)) {
      val result1 = graph.flatMap(_.process1(model, $(pullBatchSize), $(nodeAttrNum) * 4)).persist($(storageLevel))
      result1.count()
      val tempRe1 = dataset.sparkSession.createDataFrame(result1.map(r => Row(r._1, r._2.mkString(" "))), transformSchema(dataset.schema))
      val tempOutputDir1 = output + s"/$iter"
      GraphIO.save(tempRe1, tempOutputDir1)
      println(s"saved iter$iter result to $tempOutputDir1")
      iter += 1
      if (iter < $(maxIteration)) {
        model.initAttributes(result1)
        result1.unpersist()
      }
    }

    val t = SparkContext.getOrCreate().parallelize(List("1", "2"), 1)
    dataset.sparkSession.createDataFrame(t.map(x => Row(1L, x)), transformSchema(dataset.schema))

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

object MultiAttrAggregate {
  def readAttrs(path: String, nodeIndex: Int, featureIndex: Int, sep: String): RDD[(Long, Array[Float])] = {
    val df = GraphIO.loadBiGraph(path, false, srcIndex = nodeIndex, dstIndex = featureIndex, sep = sep)
    val attrs = df.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getString(1).split(" ").map(_.toFloat)))
    attrs
  }
}