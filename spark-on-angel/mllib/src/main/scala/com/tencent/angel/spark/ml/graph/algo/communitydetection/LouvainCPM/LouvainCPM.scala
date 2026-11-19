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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.LouvainCPM

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.ml.math2.vector.LongLongVector
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.Stats
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class LouvainCPM(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCommunityIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasBufferSize
  with HasDebugMode with HasUseBalancePartition with HasBalancePartitionPercent {

  final val numOpt = new IntParam(this, "numOpt", "numOpt")
  final val numFold = new IntParam(this, "numFold", "numFold")
  final val eps = new DoubleParam(this, "eps", "eps")
  final val preserveRate = new DoubleParam(this, "preserveRate", "preserveRate")
  final val useMergeStrategy = new BooleanParam(this, "useMergeStrategy", "useMergeStrategy")
  final val resolutionParameter = new DoubleParam(this, "resolutionParameter", "resolutionParameter")

  final def setNumOpt(num: Int): this.type = set(numOpt, num)

  final def setNumFold(num: Int): this.type = set(numFold, num)

  final def setEps(error: Double): this.type = set(eps, error)

  final def setPreserveRate(rate: Double): this.type = set(preserveRate, rate)

  final def setUseMergeStrategy(use: Boolean): this.type = set(useMergeStrategy, use)

  final def setResolutionParameter(resolutionParameterValue: Double): this.type = set(resolutionParameter, resolutionParameterValue)


  final def getNumOpt: Int = $(numOpt)

  final def getNumFold: Int = $(numFold)

  final def getEps: Double = $(eps)

  final def getPreserveRate: Double = $(preserveRate)

  final def getUseMergeStrategy: Boolean = $(useMergeStrategy)

  final def getResolutionParameter: Double = $(resolutionParameter)


  setDefault(numOpt, 10)
  setDefault(numFold, 3)
  setDefault(eps, 0.0)
  setDefault(preserveRate, 1.0)
  setDefault(useMergeStrategy, false)
  setDefault(resolutionParameter, 1.0)

  def this() = this(Identifiable.randomUID("louvainCPM"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    assert(dataset.sparkSession.sparkContext.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    /**
      * edges data preprocessing
      * delete null line;delete self edges;delete 0 weight edges;
      * sum the weights of multiple edges
      * repartition the edges rdd in partitionNum;
      * persist the rdd in dist
      */

    val edges = NeighborDataOps.loadEdgesWithWeight(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(weightCol), $(isWeighted), false, true, true, true)
      .persist(StorageLevel.DISK_ONLY).repartition($(partitionNum))

    val nodes = edges.flatMap { case (src, dst, _) => Iterator(src, dst) }.distinct($(partitionNum)).persist(StorageLevel.DISK_ONLY)
    val numNodes = nodes.count()
    val index = edges.flatMap(f => Array(f._1, f._2))

    val (minId, maxId, numEdges) = Stats.summarizeWithWeight(edges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges numNodes=$numNodes")

    //build the graph with LouvainCPM graph partition
    val graph: RDD[LouvainCPMGraphPartition] = LouvainCPMGraph.edgeTripleRDD2GraphPartitions(edges, storageLevel = $(storageLevel), numPartition = Option($(partitionNum)))

    // destroys the lineage
    graph.checkpoint()
    graph.foreachPartition(_ => Unit)
    edges.unpersist()

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create model
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "LouvainCPM", SparkContext.getOrCreate().hadoopConfiguration)

    val PSModel = LouvainCPMPSModel(modelContext, index, $(useBalancePartition), $(balancePartitionPercent))
    var leiden = new LouvainCPMGraph(graph, PSModel) //create the louvain object
    leiden.updateNodeWeightsToPS() //set node community with node self id;set the community weight

    val totalSum = leiden.checkTotalSum(PSModel)

    if ($(debugMode)) {
      assert(leiden.checkCommId(PSModel) == 0)
      val total = leiden.checkTotalSum(PSModel)
      assert(total == totalSum, s"$total != $totalSum")
    }

    //the main iteration precess of LouvainCPM
    var foldIter = 0
    var hasNextRun = true
    var bestModularity = -1.0

    while (hasNextRun) {
      foldIter += 1
      leiden.modularityOptimize($(numOpt), $(batchSize), $(eps), $(preserveRate), $(useMergeStrategy), $(resolutionParameter))
      val ModularityNew = leiden.getModularity($(resolutionParameter))
      println(s"----------------foldIter $foldIter modularity is: $ModularityNew-------------")

      hasNextRun &&= (ModularityNew - bestModularity > $(eps))
      if (hasNextRun) {
        leiden = leiden.folding($(batchSize), $(storageLevel))
        leiden.updateNodeCommunityFinal(nodes, $(batchSize))
        bestModularity = ModularityNew
      }

      hasNextRun &&= (foldIter < $(numFold))

      if (foldIter < $(numFold) && $(debugMode)) {
        assert(leiden.checkCommId(PSModel) == 0)
        val total = leiden.checkTotalSum(PSModel)
        assert(total == totalSum, s"$total != $totalSum")
      }
    }

    val outputSchema = transformSchema(dataset.schema)
    val result = leiden.getNodeCommunityFinal(nodes, $(batchSize))

    dataset.sparkSession.createDataFrame({
      result.map { case (id, c) => Row.fromSeq(Seq(id, c)) }
    }, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCommunityIdCol)}", LongType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
