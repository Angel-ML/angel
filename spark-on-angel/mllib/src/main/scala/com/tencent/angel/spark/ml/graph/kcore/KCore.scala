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
package com.tencent.angel.spark.ml.graph.kcore

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

/**
  * KCore algorithm implementation
  * @param uid
  */
class KCore(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasBalancePartitionPercent {

  def this() = this(Identifiable.randomUID("KCore"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)

    edges.persist(StorageLevel.DISK_ONLY)

    val maxId = edges.flatMap(f => Iterator(f._1, f._2)).max() + 1
    val minId = edges.flatMap(f => Iterator(f._1, f._2)).min()
    val index = edges.flatMap(f => Iterator(f._1, f._2))
    val numEdges = edges.count()

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = KCorePSModel.fromMinMax(minId, maxId, index, $(psPartitionNum),
      $(useBalancePartition), ${balancePartitionPercent})
    var graph = edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator(KCoreGraphPartition.apply(index, it)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.foreach(_.initMsgs(model))

    var curIteration = 0
    var numMsgs = model.numMsgs()
    var prev = graph
    println(s"numMsgs=$numMsgs")

    do {
      curIteration += 1
      graph = prev.map(_.process(model, numMsgs, curIteration == 1))
      graph.persist($(storageLevel))
      graph.count()
      prev.unpersist(true)
      prev = graph
      model.resetMsgs()
      numMsgs = model.numMsgs()
      println(s"curIteration=$curIteration numMsgs=$numMsgs")
    } while (numMsgs > 0)

    val retRDD = graph.map(_.save()).flatMap(f => f._1.zip(f._2))
      .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCoreIdCol)}", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
