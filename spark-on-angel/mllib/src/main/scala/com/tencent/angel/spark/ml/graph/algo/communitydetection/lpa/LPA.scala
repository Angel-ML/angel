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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.lpa

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.{GraphIO, Stats}
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class LPA(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasBalancePartitionPercent with HasNeedReplicaEdge with HasMaxIteration with HasInput
  with HasSrcNodeIndex with HasDstNodeIndex with HasDelimiter with HasBatchSize {
  
  final val isFixedLabels =  new BooleanParam(this, "isFixedLabels", "provided labels are fixed")
  final def getIsFixedLabels : Boolean = $(isFixedLabels)
  setDefault(isFixedLabels, true)
  final def setIsFixedLabels(bool: Boolean): this.type = set(isFixedLabels, bool)
  
  def this() = this(Identifiable.randomUID("LPA"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = GraphIO.loadEdgesFromDF(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(needReplicaEdge))
    edges.persist($(storageLevel))
    
    val (minId, maxId, numEdges) = Stats.summarize(edges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")
    
    val loadLabelTime = System.currentTimeMillis()
    val labels = if ($(input) != null) {
      GraphIO.load($(input), isWeighted = false, $(srcNodeIndex), $(dstNodeIndex), sep = $(delimiter))
        .select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1)))
    } else null
    if (labels != null) {
      labels.persist($(storageLevel))
      println(s"${labels.count()} labels provided.. " +
        s"load labels cost: ${(System.currentTimeMillis() - loadLabelTime) / 1000.0} s")
    }
    
    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId, -1,
      "lpa", SparkContext.getOrCreate().hadoopConfiguration)
    val model = LPAPSModel(modelContext, edges, $(useBalancePartition), $(balancePartitionPercent))
  
    if ($(input) != null && $(isFixedLabels)) {
      LPA.initLabelsOnPS(model, labels, $(batchSize))
    }
    
    val loadGraphTime = System.currentTimeMillis()
    val graph =
      if (($(input) == null) || (($(input) != null) && !$(isFixedLabels))) {
        edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
          .groupByKey($(partitionNum))
          .mapPartitionsWithIndex{(index, it) =>
            Iterator(LPAGraphPartition.apply(index, it))}
      }
      else {
        edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
          .groupByKey($(partitionNum))
          .mapPartitionsWithIndex{(index, it) =>
            Iterator(LPAGraphPartition.apply(index, it, model, $(batchSize)))}
      }
    
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    println(s"make graph partitions cost: ${(System.currentTimeMillis() - loadGraphTime) / 1000.0} s")
    
    val timeInitLabelID = System.currentTimeMillis()
    graph.foreach(_.initMsgs(model, $(batchSize)))
    println(s"init labels with unique id cost: ${(System.currentTimeMillis() - timeInitLabelID) / 1000.0} s")
    edges.unpersist(blocking = false)
  
    if (($(input) != null) && !$(isFixedLabels)) {
      LPA.initLabelsOnPS(model, labels, $(batchSize))
    }
    if (labels != null) {
      labels.unpersist(blocking = false)
    }
  
    var curIteration = 0
    val maxIterNum = $(maxIteration)
    var changedNum = 0L
  
    do {
      val iterationTime = System.currentTimeMillis()
      curIteration += 1
      changedNum = graph.map(_.process(model, $(batchSize))).reduce(_ + _)
      model.resetMsgs()
      println(s"curIteration=$curIteration; changedNum=$changedNum nodes changed, " +
        s"cost: ${(System.currentTimeMillis() - iterationTime) / 1000.0} s")
    } while (curIteration < maxIterNum && changedNum != 0)
    
    val retRDD = graph.flatMap(_.save(model))
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

object LPA {
  def initLabelsOnPS(model: LPAPSModel, labels: RDD[(Long, Long)], batchSize: Int): Unit = {
    println(s"start init nodes with provided labels")
    val timeInitLabel = System.currentTimeMillis()
    labels.foreachPartition{ iter =>
      iter.toArray.sliding(batchSize, batchSize).foreach{ batch =>
        val msgs = VFactory.sparseLongKeyLongVector(batch.length)
        batch.foreach(pair => msgs.set(pair._1, pair._2))
        model.initMsgs(msgs)
      }
    }
    println(s"init provided labels on ps cost: ${System.currentTimeMillis() - timeInitLabel} ms")
  }
}