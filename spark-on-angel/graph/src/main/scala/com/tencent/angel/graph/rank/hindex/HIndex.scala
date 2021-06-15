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
package com.tencent.angel.graph.rank.hindex

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.graph.utils.Stats

class HIndex(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize {

  def this() = this(Identifiable.randomUID("H-Index"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    //read edges
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    //edges's storageLevel choose  Disk_Only
    edges.persist(StorageLevel.DISK_ONLY)

    val (minId, maxId, numEdges) = edges.mapPartitions(Stats.summarizeApplyOp)
      .reduce(Stats.summarizeReduceOp)

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val modelContext = new ModelContext(${psPartitionNum}, minId, maxId + 1, -1,
      "HIndexPSModel", sc.hadoopConfiguration)
    val model = new HIndexPSModel(modelContext)
    model.init()
    val neighborTable = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
      .groupByKey($(partitionNum)).map(x => (x._1, x._2.toArray.distinct))

    neighborTable.persist($(storageLevel))

    //init msgs with each node's degree
    neighborTable.mapPartitionsWithIndex { case (index, iter) =>
      HIndexOperator.initMsgs(index, iter, model, ${batchSize})
    }.count()

    val numMsgs = model.numMsgs()
    println(s"numNodes=$numMsgs")
    model.checkpoint()

    val res = neighborTable.mapPartitionsWithIndex { case (index, iter) =>
      HIndexOperator.process(index, iter, model, ${pullBatchSize})
    }

    val ret = res.map(r => Row.fromSeq(Seq[Any](r._1, r._2, r._3, r._4)))

    dataset.sparkSession.createDataFrame(ret, transformSchema(dataset.schema))

  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        "HIndex"
      }", IntegerType, nullable = false),
      StructField(s"${
        "GIndex"
      }", IntegerType, nullable = false),
      StructField(s"${
        "WIndex"
      }", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}