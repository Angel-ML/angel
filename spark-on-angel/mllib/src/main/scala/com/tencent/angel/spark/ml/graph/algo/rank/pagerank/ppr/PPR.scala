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
package com.tencent.angel.spark.ml.graph.algo.rank.pagerank.ppr

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborTableModel
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.Stats
import com.tencent.angel.spark.ml.graph.utils.params._
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel


class PPR(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasBalancePartitionPercent with HasBatchSize with HasIsWeighted with HasWeightCol
  with HasWalkLength with HasNeedReplicaEdge with HasPullBatchSize with HasEpochNum
  with HasTopK with HasVerbose with HasIsSaveWalkPath {

  def this() = this(Identifiable.randomUID("PersonalizedPageRank"))

  private var psMatrix: PSMatrix = _
  private var output: String = _
  private var seeds: RDD[Long] = _
  private var epsilon: Double = 0d
  private var alpha: Double = 0.1d
  private var maxLength: Int = 100
  private var delta: Double = 0d
  private var degree: RDD[(Long, Int)] = _
  private var numEdges: Long = _

  def setSeeds(in: RDD[Long]): Unit = { seeds = in }
  def setOutputDir(in: String): Unit = { output = in }
  def setEpsilon(in: Double): Unit = { epsilon = in }
  def setAlpha(in: Double): Unit = { this.alpha = in }
  def setMaxLength(in: Int): Unit = { this.maxLength = in }
  def setDelta(in: Double): Unit = { this.delta = in }

  override def transform(dataset: Dataset[_]): DataFrame = {
    if ($(isWeighted)) initWeightedEdges(dataset) else initEdges(dataset)
    val beforeCheckpoint = System.currentTimeMillis()
    psMatrix.checkpoint()
    println(s"checkpoint matrix cost ${System.currentTimeMillis() - beforeCheckpoint} ms.")

    // starting nodes of random walks
    if (seeds == null) {
      println(s"take all graph nodes as ppr seeds.")
      seeds = degree.map(_._1)
    }
    seeds.persist(StorageLevel.MEMORY_ONLY)
    println(s"num seeds: ${seeds.count()}")

    // start random walking
    println(s"numEdges: $numEdges")
    val numWalks = math.ceil(math.log(numEdges) / (this.delta * math.pow(this.epsilon, 2))).toInt
    println(s"numWalks: $numWalks, maxPathLength: $maxLength")

    // -------- start sampling ------------
    val re = if ($(isSaveWalkPath))
      seeds.mapPartitionsWithIndex { case (index, seedsIter) =>
      PPROperation.sampleByBatchWithPath(psMatrix, index, seedsIter, $(pullBatchSize), numWalks, alpha, maxLength, $(topK))}
    else
      seeds.mapPartitionsWithIndex { case (index, seedsIter) =>
        PPROperation.sampleByBatch(psMatrix, index, seedsIter, $(pullBatchSize), numWalks, alpha, maxLength, $(topK))}
    dataset.sparkSession.createDataFrame(re.map(x => Row(x._1, x._2)), transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"node", LongType, nullable = false),
      StructField(s"ppr", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def initEdges(dataset: Dataset[_]): Unit = {
    val rawEdges_ = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)
    val rawEdges = if (rawEdges_.getNumPartitions < ${partitionNum}) rawEdges_.repartition($(partitionNum)) else rawEdges_.coalesce($(partitionNum))
    rawEdges.persist(StorageLevel.DISK_ONLY)

    var (minId, maxId, numEdges_) = (-1L, -1L, 0L)
    if ($(verbose)) {
      val statResults = rawEdges.mapPartitions(Stats.summarizeApplyOp).reduce(Stats.summarizeReduceOp)
      minId = statResults._1
      maxId = statResults._2
      numEdges_ = statResults._3
      println(s"minId=$minId maxId=$maxId numEdges(read edges)=$numEdges_")
    } else {
      numEdges_ = rawEdges_.count()
      println(s"numEdges(read edges)=$numEdges_")
    }

    degree = if ($(needReplicaEdge)) {
      rawEdges.flatMap(x => Iterator((x._1, 1), (x._2, 1))).reduceByKey(_ + _)
    } else
      rawEdges.map(x => (x._1, 1)).reduceByKey(_ + _)
    degree.persist($(storageLevel))
    val numNodes = degree.count()
    println(s"num src nodes: $numNodes")

    // Start PS and init the model, push aliasTable to ps
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = startPSModel(minId, maxId, numNodes)
    psMatrix = model.neighborMatrix

    val initNumNodes = model.initNeighbors(degree, $(batchSize))
    println(s"init $initNumNodes nodes' space.")

    numEdges = model.addNeighbors(rawEdges, $(batchSize), $(needReplicaEdge))
    println(s"add $numEdges edges.")
  }

  def initWeightedEdges(dataset: Dataset[_]): Unit = {
    val rawEdges_ = dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
      .filter(!_.anyNull)
      .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
      .filter(f => f._1 != f._2 && f._3 != 0)
    val rawEdges = if (rawEdges_.getNumPartitions < ${partitionNum}) rawEdges_.repartition($(partitionNum)) else rawEdges_.coalesce($(partitionNum))
      .coalesce($(partitionNum))
    rawEdges.persist(StorageLevel.DISK_ONLY)

    var (minId, maxId, numEdges_) = (-1L, -1L, 0L)
    if ($(verbose)) {
      val statResults = rawEdges.mapPartitions(Stats.summarizeApplyOp1).reduce(Stats.summarizeReduceOp)
      minId = statResults._1
      maxId = statResults._2
      numEdges_ = statResults._3
      println(s"minId=$minId maxId=$maxId numEdges(read edges)=$numEdges_")
    } else {
      numEdges_ = rawEdges_.count()
      println(s"numEdges(read edges)=$numEdges_")
    }

    degree = if ($(needReplicaEdge)) rawEdges.flatMap(x => Iterator((x._1, 1), (x._2, 1))).reduceByKey(_ + _) else rawEdges.map(x => (x._1, 1)).reduceByKey(_ + _)
    degree.persist($(storageLevel))

    val numNodes = degree.count()
    println(s"num src nodes: $numNodes")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = startPSModel(minId, maxId, numNodes)
    psMatrix = model.neighborMatrix

    var startTime = System.currentTimeMillis()
    val initNumNodes = model.initNeighbors(degree, $(batchSize), $(isWeighted))
    println(s"init $initNumNodes nodes' space, cost ${System.currentTimeMillis() - startTime} ms.")

    startTime = System.currentTimeMillis()
    numEdges = model.addWeightedNeighbors(rawEdges, $(batchSize), $(needReplicaEdge))
    println(s"add $numEdges weighted edges, cost ${System.currentTimeMillis() - startTime} ms.")

    // pull weights and calculate alias and push to ps
    startTime = System.currentTimeMillis()
    val transNumNodes = model.createAlias(degree.map(_._1), $(pullBatchSize))
    println(s"created $transNumNodes nodes' alias, cost ${System.currentTimeMillis() - startTime} ms.")
  }

  def startPSModel(minId: Long, maxId: Long, numNodes: Long): DynamicSimpleNeighborTableModel = {
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId+1, numNodes,
      "simple_neighbor", SparkContext.getOrCreate().hadoopConfiguration)
    val model = new DynamicSimpleNeighborTableModel(modelContext)
    model.init()
    model
  }

}


