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
package com.tencent.angel.spark.ml.graph.algo.connectedcomponent.wcc

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


class CutCC(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol with HasBalancePartitionPercent
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasUseBalancePartition {
  
  def this() = this(Identifiable.randomUID("WCC"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    // read edges
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    
    edges.persist(StorageLevel.DISK_ONLY)

    var graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(CutCCGraphPartition.apply(index, adjTable)))
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    
    // each partition run localCC
    graph.foreach(_.process())
    
    println(s"each partition finished panting")
    // find edges whose dst and src have different label
    // map the associated labels to the min

    val localPairs = graph.map(_.savePart()).flatMap(f => f._1.zip(f._2))
      .groupByKey($(partitionNum))
      .flatMap{ case (_, info) =>
        val minLabel = info.min
        val pMap = ArrayBuffer[(Long, Long)]()
        for (lb <- info) {
          pMap += ((lb, minLabel))
        }
        pMap.toIterator
      }.distinct().collect()
    
    val idMaps = LocalWCC.process(localPairs)
    val ss = SparkSession.builder().getOrCreate()
    val localRDD = ss.sparkContext.makeRDD(idMaps.toSeq, $(partitionNum))
    
    println("local idMaps generated, paint results")
    
    // tmp map estimated in each partition
    // remap with idMaps
    val retRDD = graph.map(_.save()).flatMap(f => f._2.zip(f._1))
      .leftOuterJoin(localRDD).map { case (prevlb, (nd, curlb)) =>
      curlb match {
        case Some(curlb) => (nd, curlb)
        case None => (nd, prevlb)
      }
    }.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

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
