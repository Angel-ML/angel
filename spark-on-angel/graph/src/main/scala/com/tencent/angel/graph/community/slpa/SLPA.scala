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

package com.tencent.angel.graph.community.slpa

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.utils.Stats
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.graph.utils.partitionstrategy.reindexpartition.MyPartition
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{LongType, StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class SLPA(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasNumMaxCommunities
  with HasMaxIteration with HasPreserveRate with HasBatchSize with HasArrayBoundsPath with HasUseBalancePartition
  with HasIsWeighted with HasWeightCol with HasUseEdgeBalancePartition with HasNeedReplicaEdge with HasBalancePartitionPercent {

  def this() = this(Identifiable.randomUID("SLPA"))

  /**
   * the main process of SLPA for the weighted/unweighted and directed/undirected graph
   * paper url : https://arxiv.org/pdf/1109.5720.pdf
   *
   * @param dataset edges data
   * @return the communities the node belongs to
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    //create edges RDD

    val rawEdges = NeighborDataOps.loadEdgesWithWeight(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(weightCol), $(isWeighted), $(needReplicaEdge), true, true, true)
    rawEdges.persist(StorageLevel.DISK_ONLY)
    val (minId, maxId, numEdges) = Stats.summarizeWithWeight(rawEdges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")
    val edges = rawEdges.map { case (src, dst, w) => (src, (dst, w)) }

    //get graph partitions using balance partition or hash partition
    var graph = if ($(useEdgeBalancePartition)) {
      val arrayBounds = SparkContext.getOrCreate().textFile($(arrayBoundsPath)).map(x => x.toLong).collect()
      val balancedPartitionNum = arrayBounds.length + 1
      edges.groupByKey(new MyPartition(balancedPartitionNum, arrayBounds))
        .mapPartitionsWithIndex((index, adjTable) =>
          Iterator(SLPAGraphPartition.apply(index, adjTable, $(preserveRate), $(batchSize))))
    } else {
      edges.groupByKey($(partitionNum)).mapPartitionsWithIndex((index, adjTable) =>
        Iterator(SLPAGraphPartition.apply(index, adjTable, $(preserveRate), $(batchSize))))
    }

    graph.persist($(storageLevel))
    //trigger action
    graph.foreachPartition(_ => Unit)

    //Start PS and init the model
    if (numEdges > 0) {
      println("start to run ps")
      PSContext.getOrCreate(SparkContext.getOrCreate())
    }
    else {
      println("the number of edges is 0 !!!")
    }


    // Create model
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "SLPA", SparkContext.getOrCreate().hadoopConfiguration)

    val data = rawEdges.flatMap(f => Iterator(f._1, f._2))
    //create ps model;init node label with node's id
    val model = SLPAPSModel(modelContext, data, $(useBalancePartition), $(balancePartitionPercent))
    graph.foreach(_.initMsgs(model))

    //label propagation process
    var curIteration = 0
    var prev = graph
    do {
      curIteration += 1
      graph = prev.map(_.process(model, curIteration))
      graph.persist($(storageLevel))
      graph.count()
      prev.unpersist(true)
      prev = graph
      model.resetMsgs()
      println(s"curIteration=$curIteration")
    } while (curIteration < $(maxIteration))

    //zip node id and community list
    val temp = graph.map(_.save()).flatMap(f => f._1.zip(f._2))
    //remove srcNode labels seen with probability < r;
    val overlapCommunities = temp.map { x =>
      val srcNode = x._1
      val nodeLabeslList = x._2
      val threshold = nodeLabeslList.map(_._2).sum / $(numMaxCommunities)
      val top_k = nodeLabeslList.sortWith(_._2 > _._2)

      if (top_k.head._2 >= threshold)
        (srcNode, top_k.take($(numMaxCommunities)).filter(_._2 >= threshold).map(y => y._1.toLong))
      else
        (srcNode, Array(top_k.head._1.toLong))
    }

    val retRDD = overlapCommunities.map(f => Row.fromSeq(Seq[Any](f._1, f._2.mkString(" "))))
    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        $(outputCoreIdCol)
      }", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
