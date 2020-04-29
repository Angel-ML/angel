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
package com.tencent.angel.spark.ml.graph.connectedcomponent.scc
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel


class SCC(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol with HasBalancePartitionPercent
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasUseBalancePartition {
  
  def this() = this(Identifiable.randomUID("SCC"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    // read edges
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    
    edges.persist(StorageLevel.DISK_ONLY)
    
    val maxId = edges.map(e => math.max(e._1, e._2)).max() + 1
    val minId = edges.map(e => math.min(e._1, e._2)).min()
    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val numEdges = edges.count()
    
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")
    
    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    
    val model = SCCPSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition), $(balancePartitionPercent))
    
    // make un-directed graph, for scc
    var graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, (dstId, true)), (dstId, (srcId, false))) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(SCCGraphPartition.apply(index, adjTable)))
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.foreach(_.initMsgs(model))
    
    var numMsgs = model.numMsgs()
    var curIteration = 0
    var totalNewFinalNum = 0L
    var newFinialNodesAll = 0L
    println(s"numMsgs=$numMsgs")
    
    do {
      curIteration += 1
      // drop single nodes, which it self makes a component
      println(s"start iteration $curIteration")
      println(s"curIteration=$curIteration start dropping single nodes")
  
      var dropCnt = 0L
      var totalDroppedNum = 0L
      do {
        dropCnt = 0L
        dropCnt = graph.map(_.dropSingle(model, numMsgs)).reduce((n1, n2) => n1 + n2)

        totalDroppedNum += dropCnt
        println(s"$dropCnt nodes dropped")
      } while (dropCnt > 0)
      println(s"curIteration=$curIteration dropped single nodes num=$totalDroppedNum")
      
      // find component with more than 1 node, using color
      
      // first
      // propagate the min label to all nodes it can reach
      // component is included in the colored super set
      val colorRecord = model.colorPSModel.readAllMsgs()
      println(s"curIteration=$curIteration start propagating propagate color label")
      var changedCnt = 0L
      var totalSelectedNum = 0L
      do {
        changedCnt = 0L
        changedCnt = graph.map(_.propagate(model, numMsgs)).reduce((n1, n2) => n1 + n2)
        totalSelectedNum += changedCnt
      } while (changedCnt > 0)
      println(s"curIteration=$curIteration color label propagated")
      
      // second
      // set the root node false,
      // if a node can reach a node that is false,
      // set it false, too
      var newFinalCnt = 0L
      totalNewFinalNum = 0L
      println(s"curIteration=$curIteration start process connected nodes")
      do {
        newFinalCnt = 0
        newFinalCnt = graph.map(_.process(model, numMsgs)).reduce((n1, n2) => n1 + n2)
        totalNewFinalNum += newFinalCnt
      } while (newFinalCnt > 0)
      println(s"curIteration=$curIteration new connected final nodes=$totalNewFinalNum")
    
      newFinialNodesAll = totalNewFinalNum + totalDroppedNum
      println(s"curIteration=$curIteration new nodes processed=$newFinialNodesAll")
      
      // node with tag true, should recover from the changed color
      println(s"curIteration=$curIteration start repainting graph")
      graph.foreach(_.repaint(model, colorRecord))
      println(s"curIteration=$curIteration graph repainted")
    } while (totalNewFinalNum > 0)
    
    val retRDD = graph.map(_.save()).flatMap(f => f._1.zip(f._2))
      .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
    
    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCoreIdCol)}", LongType, nullable = false)
    ))
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  
}
