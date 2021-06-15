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
package com.tencent.angel.graph.rank.weightedHIndex

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.Stats
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

// H-index of a weighted graph
// "Vital nodes identification in complex networks" page 51

class WeightedHIndex(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasUnDirected
  with HasBatchSize with HasPullBatchSize {

  def this() = this(Identifiable.randomUID("weighted-HIndex"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext

    //read edges
    val edges = if ($(isWeighted)) {
      dataset.select(${srcNodeIdCol}, ${dstNodeIdCol}, ${weightCol}).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2 && f._3 != 0)
    } else {
      dataset.select(${srcNodeIdCol}, ${dstNodeIdCol}).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(f => f._1 != f._2)
    }
    //edges's storageLevel choose  Disk_Only
    edges.persist(StorageLevel.DISK_ONLY)

    val (minID, maxID, numEdges) = edges.mapPartitions(Stats.summarizeApplyOp1)
      .reduce(Stats.summarizeReduceOp)

    println(s"minId=$minID maxId=$maxID numEdges=$numEdges level=${$(storageLevel)}")

    val neighborTable = edges.mapPartitions { iter =>
      iter.flatMap { case (srcId, dstId, weight) => Iterator((srcId, (dstId, weight)), (dstId, (srcId, weight)))}
    }.groupByKey($(partitionNum))
     .mapPartitions { iter =>
       iter.flatMap { x => Iterator.single(x._1, x._2.toArray.distinct)}
     }
    neighborTable.persist($(storageLevel))

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val modelContext = new ModelContext(${psPartitionNum}, minID, maxID+1, -1,
    "weightedHIndexPSModel", sc.hadoopConfiguration)
    val model = new WeightedHIndexPSModel(modelContext)
    model.init()

    //init msgs with each node's degree
    neighborTable.mapPartitionsWithIndex { case (index, iter) =>
      WeightedHIndexOperator.initMsgs(index, iter, model, ${batchSize})
    }.count()

    val numMsgs = model.numMsgs()
    println(s"numNodes=$numMsgs")
    model.checkpoint()

    val res = neighborTable.mapPartitionsWithIndex { case (index, iter) =>
      WeightedHIndexOperator.process(index, iter, model, ${pullBatchSize})
    }

    val retRDD = res.map(r => Row.fromSeq(Seq[Any](r._1, r._2)))

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))

  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        $(outputCoreIdCol)
      }", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}