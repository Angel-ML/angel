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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.lpax

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

class LPAX(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol with HasWeightCol with HasIsWeighted
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition with HasNodeAttrPath {

  def this() = this(Identifiable.randomUID("LPAX"))
  
  var nodeFlags: RDD[(Long, Int)] = _
  
  def setNodeFlag(nodeAttr: RDD[(Long, Int)]): Unit = {
    this.nodeFlags = nodeAttr
  }
  
  override def transform(dataset: Dataset[_]): DataFrame = {

    // collect all edges with weights, (dst, src, weight) for convenience to record weight
    val edgesw = if ($(isWeighted)) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(1), (row.getLong(0), row.getFloat(2))))
        .filter(f => f._1 != f._2._1)
    } else {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(1), (row.getLong(0), 1.0f)))
        .filter(f => f._1 != f._2._1)
    }
    
    // merge flags into edges (src, (dst, weight, flag))
    val edges = edgesw.join(nodeFlags).map { case (dst, ((src, w), flg)) => (dst, (src, w, flg)) }

    edgesw.persist(StorageLevel.DISK_ONLY)
    edges.persist(StorageLevel.DISK_ONLY)
    nodeFlags.persist(StorageLevel.DISK_ONLY)

    val maxId = edgesw.flatMap(f => Iterator(f._1, f._2._1)).max() + 1
    val minId = edgesw.flatMap(f => Iterator(f._1, f._2._1)).min()
    val index = edgesw.flatMap(f => Iterator(f._1, f._2._1))
    val numEdges = edgesw.count()

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model0 = LPAXPSModel.fromMinMax(minId, maxId, index, "lpax0", $(psPartitionNum), $(useBalancePartition))
    val model1 = LPAXPSModel.fromMinMax(minId, maxId, index, "lpax1", $(psPartitionNum), $(useBalancePartition))

    // (dst, (src, weight, flag))
    var graph = edges.flatMap(f => Iterator((f._1, (f._2._1, f._2._2, f._2._3))))
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator(LPAXGraphPartition.apply(index, it)))


    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.foreach(_.initMsgs(model0, model1))

    var curIteration = 0
    var numMsgs = model0.numMsgs() + model1.numMsgs()
    var prev = graph

    println(s"numMsgs = $numMsgs")

    do {
      curIteration += 1
      graph = prev.map(_.process(model0, model1))
      graph.persist($(storageLevel))
      graph.count()
      prev.unpersist(true)
      prev = graph
      model0.resetMsgs()
      model1.resetMsgs()
      numMsgs = model0.numMsgs() + model1.numMsgs()
      println(s"curIteration=$curIteration numMsgs=$numMsgs")
    } while (curIteration < 10)

    val retRDD = graph.map(_.save).flatMap(f => f._1.zip(f._3))
      .sortBy(_._2, false)
      .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCoreIdCol)}", FloatType, nullable = false)
      
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
