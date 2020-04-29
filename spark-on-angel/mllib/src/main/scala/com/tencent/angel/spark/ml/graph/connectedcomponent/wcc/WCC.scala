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
package com.tencent.angel.spark.ml.graph.connectedcomponent.wcc
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel


class WCC(override val uid: String) extends Transformer
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

		val maxId = edges.map(e => math.max(e._1, e._2)).max() + 1
		val minId = edges.map(e => math.min(e._1, e._2)).min()
		val nodes = edges.flatMap(e => Iterator(e._1, e._2))
		val numEdges = edges.count()

		println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

		// Start PS and init the model
		println("start to run ps")
		PSContext.getOrCreate(SparkContext.getOrCreate())

		val model = WCCPSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition), $(balancePartitionPercent))

		// make un-directed graph, for wcc
		var graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
			.groupByKey($(partitionNum))
			.mapPartitionsWithIndex((index, adjTable) => Iterator(WCCGraphPartition.apply(index, adjTable)))
		graph.persist($(storageLevel))
		graph.foreachPartition(_ => Unit)
		graph.foreach(_.initMsgs(model))
		
		var numMsgs = model.numMsgs()
		var curIteration = 0
		println(s"numMsgs=$numMsgs")
		
		// each node change its label into the min id of its neighbors (including itself).
    var changedCnt = 0
		do {
			curIteration += 1
      changedCnt = 0
			changedCnt = graph.map(_.process(model, numMsgs, curIteration == 1)).reduce((n1, n2) => n1 + n2)
			graph.persist($(storageLevel))
			graph.count()
			println(s"curIteration=$curIteration numMsgs=$changedCnt")
		} while (changedCnt > 0)

		val retRDD = graph.map(_.save()).flatMap(f => f._1.zip(f._2))
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
