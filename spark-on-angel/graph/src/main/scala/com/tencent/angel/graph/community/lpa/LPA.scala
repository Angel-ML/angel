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

import com.tencent.angel.graph.utils.io.Log
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class LPA(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition {

  final val maxIter = new IntParam(this, "maxIter", "maxIter")

  final def setMaxIter(numIters: Int): this.type = set(maxIter, numIters)

  setDefault(maxIter, 10)

  def this() = this(Identifiable.randomUID("LPA"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)

    edges.persist(StorageLevel.DISK_ONLY)

    val index = edges.flatMap(f => Iterator(f._1, f._2))
    val maxId = index.max() + 1
    val minId = index.min()
    val numEdges = edges.count()

    Log.withTimePrintln(s"minId=$minId maxId=$maxId numEdges=$numEdges storageLevel=${storageLevel}")

    // Start PS and init the model
    Log.withTimePrintln("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = LPAPSModel.fromMinMax(minId, maxId, index, $(psPartitionNum), $(useBalancePartition))

    var graph = edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator(LPAPartition.apply(index, it)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)

    var numChanged = graph.map(_._1.initMsgs(model)).reduce(_ + _)

    //Loop
    var i = 0
    var prev = graph
    val maxIterations = $(maxIter)
    Log.withTimePrintln(s"numChanged = $numChanged")

    do {
      i += 1
      graph = prev.map(_._1.process(model))
      graph.persist($(storageLevel))
      numChanged = graph.map(_._2).reduce(_ + _)
      graph.count()
      prev.unpersist(true)
      prev = graph
      model.resetMsgs()
      Log.withTimePrintln(s"LPA finished iteration + $i, and $numChanged nodes changed  lpa label")
    } while (i < maxIterations && numChanged > 0)

    val retRDD = graph.map(_._1.save).flatMap(f => f._1.zip(f._2))
      .sortBy(_._2)
      .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"$outputNodeIdCol", LongType, nullable = false),
      StructField(s"$outputCoreIdCol", LongType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
