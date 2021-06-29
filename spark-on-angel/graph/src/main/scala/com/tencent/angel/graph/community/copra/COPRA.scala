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
package com.tencent.angel.graph.community.copra

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.graph.utils.Stats
import com.tencent.angel.graph.utils.partitionstrategy.reindexpartition.MyPartition
import com.tencent.angel.graph.community.OverlapNMI
import scala.collection.mutable

class COPRA(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasNumMaxCommunities
  with HasMaxIteration with HasPreserveRate with HasBatchSize with HasArrayBoundsPath
  with HasIsWeighted with HasWeightCol with HasUseEdgeBalancePartition with HasUseBalancePartition with HasBalancePartitionPercent{


  def this() = this(Identifiable.randomUID("COPRA"))


  override def transform(dataset: Dataset[_]): DataFrame = {

    val edges = NeighborDataOps.loadEdgesWithWeight(dataset,$(srcNodeIdCol), $(dstNodeIdCol), $(weightCol),$(isWeighted))
    edges.persist(StorageLevel.DISK_ONLY)
    val (minId, maxId, numEdges) = Stats.summarizeWithWeight(edges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")


    def variance(in: Array[Int]): (Float, Float) = {
      val average = in.map(_.toLong).sum / in.length
      val variance = in.map(a=> (a-average)*(a-average)).sum
      (average, variance)
    }

    //    val beforePartition = edges.mapPartitionsWithIndex((num, it) => Iterator(it.size)).collect()
    //    val (a, v) = variance(beforePartition)
    //    println(s"read partitions: ${beforePartition.length}, average and variance of numEdges are: $a and $v ")

    // --------- get graph partitions using balance partition or hash partition -------------
    var graph = if ($(useEdgeBalancePartition)) {
      val arrayBounds = SparkContext.getOrCreate().textFile($(arrayBoundsPath)).map(x => x.toLong).collect()
      val balancedPartitionNum = arrayBounds.length + 1
      edges.flatMap(f => Iterator((f._1, (f._2, f._3)), (f._2, (f._1, f._3))))
        .groupByKey(new MyPartition(balancedPartitionNum, arrayBounds))
        .mapPartitionsWithIndex((index, it) =>
          Iterator(COPRAGraphPartition.apply(index, it, $(numMaxCommunities), $(preserveRate), $(batchSize))))
    } else {
      edges.flatMap(f => Iterator((f._1, (f._2, f._3)), (f._2, (f._1, f._3))))
        .groupByKey($(partitionNum)).mapPartitionsWithIndex((index, it) =>
        Iterator(COPRAGraphPartition.apply(index, it, $(numMaxCommunities), $(preserveRate), $(batchSize))))
    }

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    //    val analysis = graph.map(_.analysis()).collect()
    //    val (a_, v_) = variance(analysis)
    //    println(s"graph partitions: ${graph.getNumPartitions}, average and variance of numEdges are: $a_ and $v_ ")

    // ---------------- Start PS and init the model -------------------------
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create model
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "COPRA", SparkContext.getOrCreate().hadoopConfiguration)

    //val model = COPRAPSModel.fromMinMax(minId, maxId, data, $(psPartitionNum),false)
    val data = edges.flatMap(f => Iterator(f._1, f._2))
    val model = COPRAPSModel(modelContext, data, $(useBalancePartition), $(balancePartitionPercent))

    graph.foreach(_.initMsgs(model))

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

    val temp = graph.map(_.save()).flatMap(f => f._1.zip(f._2))
    val overlapCommunities = temp.map(x=>(x._1, x._2.map(_._1)))

    def calcNMI(realCommunitiesFile: String, overlapCommunities: RDD[(Long, Array[Long])]): Unit = {
      // 获取真实社区信息
      def getRealCommunities(realCommunitiesFile: String) : (RDD[(String, Array[Long])], RDD[(Long,Array[String])]) = {
        val node2com = SparkContext.getOrCreate().textFile(realCommunitiesFile)
          .map(x => x.trim.split("\t"))
          .map(x => (x(0).toLong, x(1).split(" ")))
          .persist(StorageLevel.MEMORY_AND_DISK_SER).setName("node2com")

        val com2node = node2com.flatMap(item => {
          var res = new mutable.ArrayBuffer[(String,Array[Long])]
          val (vid, labels) = item
          labels.foreach(label => {res += ((label, Array(vid)))})
          res
        }).reduceByKey(_++_).persist(StorageLevel.MEMORY_AND_DISK_SER).setName("com2node")
        ( com2node, node2com)
      }

      val (realCommunities, vidLabels) = getRealCommunities(realCommunitiesFile)
      val communitySize = overlapCommunities.mapPartitions(iter => for((label, nodes) <- iter) yield (label, nodes.size))
        .persist(StorageLevel.MEMORY_AND_DISK_SER).setName("communitySize")
      val nmi = new OverlapNMI()
      val com = overlapCommunities

      val x = com.flatMap(item => {
        var res = new mutable.ArrayBuffer[(Long,Array[Long])]
        val (vid, labels) = item
        labels.foreach(label => {res += ((label, Array(vid)))})
        res
      }).reduceByKey(_++_).sortByKey()
      val y = realCommunities.sortByKey().map(x=>x._2)

      val verticesNum = overlapCommunities.count()
      val nmi_max = nmi.NMI_max(x.map(_._2), y, verticesNum)
      val nmi_flk = nmi.NMI_LFK(x.map(_._2), y, verticesNum)
      val (pureRate, _) = nmi.pureRate(overlapCommunities, vidLabels, communitySize)
      println(s"NMI_MAX: $nmi_max, NMI_LFK: $nmi_flk, pureRate: $pureRate")
    }

    //    val realCommunitiesFile = "path to community file"
    //    calcNMI(realCommunitiesFile, overlapCommunities)

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
