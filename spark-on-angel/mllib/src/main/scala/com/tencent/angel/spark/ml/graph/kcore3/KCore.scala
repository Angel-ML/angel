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
package com.tencent.angel.spark.ml.graph.kcore3

import com.tencent.angel.spark.ml.graph.params.{HasDstNodeIdCol, HasOutputCoreIdCol, HasOutputNodeIdCol, HasPartitionNum, HasSrcNodeIdCol, HasStorageLevel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class KCore(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum {

  def this() = this(Identifiable.randomUID("KCore"))


  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .map(row => (row.getLong(0), row.getLong(1))).filter(f => f._1 != f._2)

    edges.persist(StorageLevel.DISK_ONLY)

    val maxId = edges.flatMap(f => Array(f._1, f._2)).max() + 1
    val minId = edges.flatMap(f => Array(f._1, f._2)).min()
    val numEdges = edges.count()

    println(s"maxId=$maxId numEdges=$numEdges")

    val model = KCorePSModel.fromMinMax(minId, maxId)

    var graph = edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
      .groupByKey($(partitionNum))
      .mapPartitions(iter => Iterator(KCoreGraphPartition.apply(iter)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.foreach(p => p.init(model))

    var K = 1
    var numModified = 0
    var numActive = 0
    do {
      val pair = graph.map(_.process(model, K))
        .reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))
      numModified = pair._1
      numActive = pair._2
      println(s"numModified=$numModified numActive=$numActive")
      if (numModified == 0) K += 1
    } while (numActive > 0)

    val retRDD = graph.map(_.save(model)).flatMap(f => f._1.zip(f._2))
      .map { case (node, core) => Row.fromSeq(Seq[Any](node, core)) }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(retRDD, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCoreIdCol)}", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
