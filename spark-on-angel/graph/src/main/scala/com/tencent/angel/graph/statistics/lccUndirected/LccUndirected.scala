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
package com.tencent.angel.graph.statistics.lccUndirected

import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.statistics.commonfriends.CommonFriendsPSModel
import com.tencent.angel.graph.utils.Stats
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import com.tencent.angel.graph.utils.params._

class LccUndirected(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasCompressCol
  with HasIsCompressed with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasCompressIndex
  with HasInput with HasExtraInputs with HasDelimiter {

  def this() = this(Identifiable.randomUID("lccUndirected"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sc = dataset.sparkSession.sparkContext

    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")

    println(s"======load edges from the first input======")
    val firstEdges: RDD[(Long, Long)] = LccOperator.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol))

    println(s"======sample edges======")
    println(firstEdges.take(10).mkString(","))

    println(s"======convert edges to neigbors tables======")
    val firstNeighbors: RDD[(Long, Array[Long])] =
      NeighborDataOps.edges2NeighborTable(firstEdges, $(partitionNum)).persist($(storageLevel))

    println(s"======sample neighbor tables======")
    firstNeighbors.take(10).foreach { case (src, neighbors) =>
      println(s"src = $src, neighbors = ${neighbors.mkString(",")}")
    }

    println(s"======statistics of the data======")
//    val stats = CommonFriendsOperator.statsByNeighborTable(firstNeighbors)
    val (minId, maxId, numEdges) = firstEdges.mapPartitions(Stats.summarizeApplyOp).reduce(Stats.summarizeReduceOp)
    println(s"minId: $minId, maxId: $maxId, numEdges: $numEdges")

    println(s"======start parameter server======")
    val psStartTime = System.currentTimeMillis()
    startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"======calc degree======")
    val degree = LccOperator.calcDegree(firstNeighbors, $(batchSize)).persist($(storageLevel))
    val degreeCount = degree.count()
    println(s"num nodes from degree: $degreeCount")

    println(s"======push degree to ps======")
    val initDegreeStartTime = System.currentTimeMillis()
    val degreeVec = LccOperator.initDegree(minId, maxId + 1, degree, $(psPartitionNum), ${batchSize})
    println(s"initializing degree costs ${System.currentTimeMillis() - initDegreeStartTime} ms")

    println(s"======push neighbor tables to parameter server======")
    val initTableStartTime = System.currentTimeMillis()
    val psModel = CommonFriendsPSModel(maxId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum), minIndex = minId)
    psModel.initLongNeighborTable(firstNeighbors, firstEdges.flatMap(x =>Iterator(x._1, x._2)))
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

//    CommonFriendsOperator.testPS(firstNeighbors, psModel, 10)
//    val checkValid = CommonFriendsOperator.checkValid(firstNeighbors, psModel, 10)
//    require(checkValid, s"result with executor RDD and that with PS are different")

    println(s"======calc triangles======")
    val beforeCalcTriangle = System.currentTimeMillis()
    val triangles = firstNeighbors.mapPartitionsWithIndex { case (partId, iter) =>
      LccOperator.runNeighborPartition_1(iter, partId, psModel)
    }.reduceByKey(_ + _).persist(${storageLevel})
    triangles.count()
    println(s"calc triangle cost ${System.currentTimeMillis() - beforeCalcTriangle} ms.")
    firstNeighbors.unpersist()

//    println(s"======destroy neighborTable psMatrix======")
//    psModel.neighborTable.psMatrix.destroy()

    println(s"======calc lcc======")
    val rawResult = triangles.mapPartitionsWithIndex { case (index, iter) => LccOperator.calcLcc(index, iter, degreeVec, ${pullBatchSize}) }

    println(s"======sample results======")
    rawResult.take(10).foreach { row =>
      println(s"src = ${row.getLong(0)}, numTriangles = ${row.getInt(1)}, lcc = ${row.getFloat(2)}, effective_size = ${row.getFloat(3)}")
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rawResult, outputSchema)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField("node", LongType, nullable = false),
      StructField("numTriangles", IntegerType, nullable = false),
      StructField("lcc", FloatType, nullable = false),
      StructField("effectiveSize", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object LccUndirected {
  
}
