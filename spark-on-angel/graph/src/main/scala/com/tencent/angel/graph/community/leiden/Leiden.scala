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
package com.tencent.angel.graph.community.leiden

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.utils.{Delimiter, Stats}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.spark.ml.util.Utils.{runOnTimer, secondTimer}

class Leiden(override val uid: String) extends Transformer with LeidenParams {

  def this() = this(Identifiable.randomUID("Leiden"))

  def this(params: Map[String, String]) = {
    this(Identifiable.randomUID("Leiden"))
    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val outputCommunityIdCol = params.getOrElse("outputCommunityIdCol", "comm")
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "10").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val srcNodeIndex = params.getOrElse("srcNodeIndex", "0").toInt
    val dstNodeIndex = params.getOrElse("dstNodeIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val itemSep = Delimiter.parse(params.getOrElse("itemSep", Delimiter.SPACE_VAL))
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val maxIteration = params.getOrElse("maxIteration", "10").toInt
    val maxOptimization = params.getOrElse("maxOptimization", "10").toInt
    val batchSize = params.getOrElse("batchSize", "1000").toInt
    val gamma = params.getOrElse("gamma", "0.1").toFloat
    val theta = params.getOrElse("theta", "0.01").toFloat

    setPartitionNum(partitionNum)
      .setPSPartitionNum(psPartitionNum)
      .setStorageLevel(storageLevel)
      .setIsWeighted(isWeighted)
      .setBatchSize(batchSize)
      .setTheta(theta)
      .setGamma(gamma)
      .setMaxIteration(maxIteration)
      .setMaxOptimization(maxOptimization)
      .setInput(input)
      .setOutput(output)
      .setSrcNodeIndex(srcNodeIndex)
      .setDstNodeIndex(dstNodeIndex)
      .setWeightIndex(weightIndex)
      .setItemSep(itemSep)
      .setOutputCommunityIdCol(outputCommunityIdCol)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    assert(dataset.sparkSession.sparkContext.getCheckpointDir.nonEmpty, "set checkpoint dir first")


    val edges = NeighborDataOps.loadEdgesWithWeight(dataset, $(srcNodeIdCol),
        $(dstNodeIdCol), $(weightCol), $(isWeighted), false, true, true, true)
      .persist($(storageLevel))

    val (minId, maxId, numEdges) = Stats.summarizeWithWeight(edges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    val ((graphs,gCount ), fCost) = runOnTimer(() => {
      val graphs = LeidenGraph.fromEdges(edges, numPartition = Some($(partitionNum)),
        batchSize = $(batchSize)).persist($(storageLevel))
      val gCount = graphs.count()
      (graphs, gCount)
    })
    println(s"create partition graphs $gCount, cost ${secondTimer(fCost)}s.")
    edges.unpersist(false)

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "leiden", SparkContext.getOrCreate().hadoopConfiguration)

    val model = LeidenPSModel(modelContext)
    val leiden = new LeidenGraph(graphs, model)
    val (_, iCost) = runOnTimer(() => leiden.initPSModel())
    println(s"initialize ps vector model cost ${secondTimer(iCost)}s.")

    val refineSet = leiden.graphs
      .map(g => g.superNodes -> Iterator.range(g.min.toInt, (g.max + 1).toInt).map(_.toLong).toArray)
      .persist($(storageLevel))
    refineSet.count()

    this.communityDivision(leiden, refineSet)

    val outputSchema = transformSchema(dataset.schema)
    val rows = refineSet.flatMap { case (nodes, comms) => nodes.zip(comms) }
      .sortByKey()
      .map { case (id, c) => Row.fromSeq(Seq(id, c)) }
    dataset.sparkSession.createDataFrame(rows, outputSchema)
  }

  override def copy(extra: ParamMap): Leiden = copyValues(new Leiden(uid), extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCommunityIdCol)}", LongType, nullable = false)
    ))
  }

  private def communityDivision(leiden: LeidenGraph, refineSet:RDD[(Array[Long], Array[Long])]): Unit = {
    var nodeCount = leiden.nodeCount()
    println(s"total node count $nodeCount.")

    var step = 0
    while (step < $(maxIteration) && !leiden.graphs.isEmpty()) {
      val start = System.currentTimeMillis()
      step += 1
      println(s"\n====================== iteration $step ======================")
      val (count, oCost) = runOnTimer(() => {
        leiden.optimize($(gamma), $(maxOptimization))
        leiden.communityCount()
      })
      println(s"[move nodes fast] community count $count cost ${secondTimer(oCost)}s.")

      //  done |P| = |V (G)|
      if (nodeCount == count) {
        step = $(maxIteration)
      } else {
        val (_, rCost) = runOnTimer(() => leiden.refineGraph($(gamma), $(theta), $(batchSize), $(storageLevel)))
        println(s"[refine] run cost ${secondTimer(rCost)}s")

        // update community of super-node
        val (_, uCost) = runOnTimer(() => Leiden.updatePartitionSet(refineSet, leiden.model))
        println(s"[update] refine partition set cost ${secondTimer(uCost)}s.")

        val (_, aCost) = runOnTimer(() => leiden.aggregateGraph($(batchSize), $(storageLevel)))
        println(s"[aggregate] run cost ${secondTimer(aCost)}s.")

        // resign community id 0-|v|
        val (_, iCost) = runOnTimer(() => leiden.initPSModel(true))
        println(s"[update ps] ps vector model cost ${secondTimer(iCost)}s.")

        // resign community id 0-|v|
        val (_, nCost) = runOnTimer(() => Leiden.updatePartitionSet(refineSet, leiden.model))
        println(s"[update] partition set cost ${secondTimer(nCost)}s.")

        nodeCount = leiden.nodeCount()
      }

      val iterCost = System.currentTimeMillis() - start
      println(s"new community count $nodeCount, iteration cost ${secondTimer(iterCost)}s.")
    }
  }
}

object Leiden {
  def loadEdges(dataset: Dataset[_],
                isWeighted: Boolean = false,
                srcNodeIdCol: String,
                dstNodeIdCol: String,
                weightCol: String,
                partitionNum: Int = 100
               ): RDD[((Long, Long), Float)] = {
    println("input data:")
    dataset.show(10, truncate = false)
    val rdd = if (isWeighted) {
      dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd.filter(row => !row.anyNull).map { row =>
        (row.getLong(0), row.getLong(1), row.getFloat(2))
      }
    } else {
      dataset.select(srcNodeIdCol, dstNodeIdCol).rdd.filter(row => !row.anyNull).map { row =>
        (row.getLong(0), row.getLong(1), 1.0f)
      }
    }
    rdd.map { case (src, dst, wgt) =>
      if (src < dst) ((src, dst), wgt) else ((dst, src), wgt)
    }.reduceByKey(_ + _, partitionNum)
  }


  def updatePartitionSet(set: RDD[(Array[Long], Array[Long])], model: LeidenPSModel): Unit = {
    set.foreach { case (_, comms) =>
      val old2new = model.getOld2New(comms.distinct)
      comms.indices.map(i => i -> old2new.get(comms(i)))
        .foreach { case (i, change) => comms(i) = change }
    }
  }
}