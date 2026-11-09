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
package com.tencent.angel.spark.ml.graph.algo.statistics.sssp

import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class MSSP(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputDistanceCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize with HasWeightCol {

  final val sources = new Param[Array[Long]](this, "sources", "sources")

  final def setSource(nodes: Array[Long]): this.type = set(sources, nodes)

  def this() = this(Identifiable.randomUID("MSSP"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    var edges: RDD[(Long, Long, Float)] = null
    if (dataset.schema.filter(f => f.name == $(weightCol)).size == 1) {
      edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol))
        .rdd.map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
    } else {
      edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol))
        .rdd.map(row => (row.getLong(0), row.getLong(1), 1.0f))
    }

    edges.persist(StorageLevel.DISK_ONLY)

    val allNodes = edges.flatMap(f => Array(f._1, f._2)).distinct()
    val maxId = allNodes.max() + 1
    println(s"maxId=$maxId sources=", $(sources).mkString(","))
    val model = MSSPPSModel.fromMaxId($(sources).length, maxId)

    val graph = edges.map(sd => (sd._1, (sd._2, sd._3))).groupByKey($(partitionNum))
      .mapPartitions(iter => Iterator.single(MSSPGraphPartition.apply(iter)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)

    model.init($(sources), allNodes)

    var numModified = Array.fill($(sources).length)(-1)
    var numActive = 0
    do {
      val modifiedAndActive = graph.map(_.process(model, numModified.clone()))
        .reduce{(v1, v2) =>
          (v1._1.zip(v2._1).map{pair => pair._1 + pair._2},
                           v1._2 + v2._2)}
      numModified = modifiedAndActive._1
      numActive = modifiedAndActive._2
      model.resetVersion()
      println("activeSources = %d, numModified = %d, numActive = %d".
        format(numModified.count(_ != 0), numModified.sum, numActive))
    } while (numModified.sum > 0)

    val retRDD = allNodes.mapPartitions {
      case iter =>
        val nodes = iter.toArray
        val dists = model.pullDistances(nodes.clone())
        Iterator(nodes.map(node => (node, dists.map(_.get(node)))
        ))
    }.flatMap(it => it)
      .map {
        case (node, dists) =>
          Row.fromSeq(Seq[Any](node) ++ dists.toSeq.asInstanceOf[Seq[Any]])
      }
    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(retRDD, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      Seq(StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false)) ++
      $(sources).map(src => StructField(s"${$(outputDistanceCol)}$src", FloatType, nullable = false))
    )
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
