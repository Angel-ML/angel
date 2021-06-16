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

package com.tencent.angel.graph.connectedcomponent.wcc

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.{GraphIO, Stats}
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class WCC(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol with HasBalancePartitionPercent
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasUseBalancePartition with HasNeedReplicaEdge {
  
  final val batchNum = new IntParam(this, "batchNum", "batchNum")
  final def setbatchNum(num: Int): this.type = set(batchNum, num)
  setDefault(batchNum, 4)
  
  def this() = this(Identifiable.randomUID("WCC"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    // read edges
    val edges = GraphIO.loadEdgesFromDF(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(needReplicaEdge))
    edges.persist($(storageLevel))
    
    val (minId, maxId, numEdges) = Stats.summarize(edges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")
    
    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId, -1,
      "labels",  SparkContext.getOrCreate().hadoopConfiguration)
    val model = WCCPSModel(modelContext, edges, $(useBalancePartition), $(balancePartitionPercent))
    
    // make un-directed graph, for wcc
    var graph = edges.groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator((0, WCCPartition.apply(index, adjTable))))
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.count()
    
    edges.unpersist(blocking = false)
    
    graph.foreach(_._2.initMsgs(model, $(batchSize)))
    graph.foreachPartition(_ => Unit)
    graph.count()
    
    var numMsgs = model.numMsgs()
    var curIteration = 0
    var prev = graph
    println(s"numMsgs=$numMsgs")
    
    // each node change its label into the min id of its neighbors (including itself).
    var changedCnt = 0
    do {
      curIteration += 1
      changedCnt = 0
      //changedCnt = graph.map(_.process(model, numMsgs, curIteration == 1)).reduce((n1, n2) => n1 + n2)
      graph = prev.map(_._2.process(model, $(pullBatchSize), numMsgs, curIteration == 1))
      graph.persist($(storageLevel))
      graph.count()
      changedCnt = graph.map(_._1).reduce((n1, n2) => n1 + n2)
      prev.unpersist(true)
      prev = graph
      
      println(s"curIteration=$curIteration numMsgs=$changedCnt")
    } while (changedCnt > 0)
    
    val retRDD = graph.map(_._2.save()).flatMap(f => f._1.zip(f._2))
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
