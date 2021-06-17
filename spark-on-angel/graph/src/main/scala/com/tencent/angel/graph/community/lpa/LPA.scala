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

package com.tencent.angel.graph.community.lpa

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.io.Log
import com.tencent.angel.graph.utils.{GraphIO, Stats}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class LPA(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasBalancePartitionPercent with HasNeedReplicaEdge {
  
  final val maxIter = new IntParam(this, "maxIter", "maxIter")
  final def setMaxIter(numIters: Int): this.type = set(maxIter, numIters)
  setDefault(maxIter, Int.MaxValue)
  
  def this() = this(Identifiable.randomUID("LPA"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = GraphIO.loadEdgesFromDF(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(needReplicaEdge))
    edges.persist($(storageLevel))
    
    val (minId, maxId, numEdges) = Stats.summarize(edges)
    Log.withTimePrintln(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")
    
    // Start PS and init the model
    Log.withTimePrintln("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId, -1,
      "lpa", SparkContext.getOrCreate().hadoopConfiguration)
    val model = LPAPSModel(modelContext, edges, $(useBalancePartition), $(balancePartitionPercent))
    
    var graph = edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator((0L, LPAGraphPartition.apply(index, it))))
    
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.count()
    
    graph.foreach(_._2.initMsgs(model))
    graph.foreachPartition(_ => Unit)
    graph.count()
    
    edges.unpersist(blocking = false)
    
    var curIteration = 0
    var numMsgs = model.numMsgs()
    var prev = graph
    val maxIterNum = $(maxIter)
    var changedNum = 0L
    println(s"numMsgs = $numMsgs")
    
    do {
      curIteration += 1
      graph = prev.map(_._2.process(model, numMsgs))
      graph.persist($(storageLevel))
      graph.count()
      changedNum = graph.map(_._1).reduce(_ + _)
      prev.unpersist(false)
      prev = graph
      model.resetMsgs()
      numMsgs = model.numMsgs()
      Log.withTimePrintln(s"LPA finished iteration $curIteration; $changedNum  nodes changed  lpa label")
    } while (curIteration < maxIterNum && changedNum != 0)
    
    val retRDD = graph.map(_._2.save).flatMap(f => f._1.zip(f._2))
      .sortBy(_._2)
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