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
package com.tencent.angel.graph.statistics.commonfriends.incComFriends

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.{BatchIter, GraphIO}
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.utils.ArrayUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

class IncComFriends(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasCompressCol
  with HasIsCompressed with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasCompressIndex
  with HasInput with HasExtraInputs with HasDelimiter with HasUseBalancePartition
  with HasMinNodeId with HasMaxNodeId with HasSetCheckPoint {

  def this() = this(Identifiable.randomUID("IncCommonFriends"))

  private var incEdges: RDD[(Long, Long)] = _
  def setIncEdges(in: RDD[(Long, Long)]): Unit = {
    this.incEdges = in
  }

  private var outputPath: String = _
  def setOutputPath(in: String): Unit = { this.outputPath = in }

  private var maxComFriendsNum: Int = Int.MaxValue
  def setMaxComFriendsNum(in: Int): Unit = { this.maxComFriendsNum = in }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sc = dataset.sparkSession.sparkContext

    println(s"====== load edges ======")
    val firstEdges: RDD[(Long, Long, Int)] = IncComFriends.loadComFriendsResults(dataset, ${partitionNum})
    val min = ${minNodeId}
    val max = ${maxNodeId}

    val tagged = incEdges.flatMap(x => Iterator(x._1, x._2))
    val minId = math.min(tagged.min(), min)
    val maxId = math.max(tagged.max(), max)
    val taggedNodes = incEdges.map(_._1)

    println(s"====== start parameter server ======")
    val psStartTime = System.currentTimeMillis()
    startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    val nbrModelContext = new ModelContext(${psPartitionNum}, minId, maxId + 1, -1,
      "neighbor", sc.hadoopConfiguration)
    val tagModelContext = new ModelContext(${psPartitionNum}, minId, maxId + 1, -1,
      "tag", sc.hadoopConfiguration)
    val psModel = new IncComFriendsPSModel(nbrModelContext, tagModelContext)
    psModel.init()

    val tagNum = psModel.updateTag(taggedNodes)
    println(s"init $tagNum nodes.")

    // push neighbor table to ps
    psModel.initNeighborsWithInfo(firstEdges, ${batchSize})
    psModel.initNeighbors(incEdges, ${batchSize})
    psModel.trans()
    val ckpStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint cost ${System.currentTimeMillis() - ckpStartTime} ms.")

    // filter and save the unchanged edges, no deleted edges
    val taggedEdges = firstEdges.mapPartitionsWithIndex { case (index, iter) =>
      BatchIter(iter, ${pullBatchSize}*3).flatMap { batchIter =>
        val pullNodes = new mutable.HashSet[Long]()
        pullNodes ++= batchIter.flatMap(x => Iterator(x._1, x._2))
        val beforePull = System.currentTimeMillis()
        val pulled = psModel.readTag(pullNodes.toArray)
        val time = System.currentTimeMillis() - beforePull
        println(s"distinguish edges: partition $index, pull ${pullNodes.size} nodes' tag, cost $time ms.")
        batchIter.map { case (src, dst, num) =>
          if (pulled.get(src) == 1 || pulled.get(dst) == 1) (src, dst, -2) else (src, dst, num)
        }
      }
    }.persist(${storageLevel})

    val schema = StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(dstNodeIdCol), LongType, nullable = false),
      StructField($(numCommonFriendsCol), IntegerType, nullable = false)
    ))
    val partRe = taggedEdges.mapPartitions { iter =>
      iter.flatMap { x => if (x._3 > -2) Iterator.single(Row(x._1, x._2, x._3)) else Iterator.empty }
    }
    GraphIO.save(dataset.sparkSession.createDataFrame(partRe, schema), outputPath)

    val partRe1 = taggedEdges.filter(_._3 == -2).mapPartitionsWithIndex { case (index, iter) =>
      IncComFriends.runEdgePartition(index, iter, ${pullBatchSize}, psModel, maxComFriendsNum)
    }
    GraphIO.appendSave(dataset.sparkSession.createDataFrame(partRe1, schema), outputPath)

    val partRe2 = incEdges.mapPartitionsWithIndex { case (index, iter) =>
      IncComFriends.runIncEdgePartition(index, iter, ${pullBatchSize}, psModel, maxComFriendsNum)
    }
    GraphIO.appendSave(dataset.sparkSession.createDataFrame(partRe2, schema), outputPath)

    // useless return
    val t = SparkContext.getOrCreate().parallelize(List((0L, 0L, 0)), 1)
    dataset.sparkSession.createDataFrame(t.map(x => Row(x._1, x._2, x._3)), transformSchema(dataset.schema))
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(dstNodeIdCol), LongType, nullable = false),
      StructField($(numCommonFriendsCol), IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object IncComFriends {

  def loadComFriendsResults(dataset: Dataset[_], partitionNum: Int): RDD[(Long, Long, Int)] = {
    dataset.select("src", "dst", "weight").rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        Iterator.single(row.getLong(0), row.getLong(1), row.getFloat(2).toInt)
      }
    }.coalesce(partitionNum)
  }

  def runEdgePartition(partId: Int, iter: Iterator[(Long, Long, Int)], batchSize: Int,
                       psModel: IncComFriendsPSModel, maxComFriendsNum: Int=Int.MaxValue) = {
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"edgePartition: partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes = new mutable.HashSet[Long]()
      pullNodes ++= batchIter.flatMap(x => Iterator(x._1, x._2))
      val beforePullTs = System.currentTimeMillis()
      val pulled = psModel.getNeighbors(pullNodes.toArray)
      println(s"edgePartition: partition $partId: process ${batchIter.length} edges, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      batchIter.map { case (src, dst, _) =>
        Row(src, dst, ArrayUtils.intersectCountWithLimits(pulled.get(src), pulled.get(dst), maxComFriendsNum))
      }
    }
  }

  def runIncEdgePartition(partId: Int, iter: Iterator[(Long, Long)], batchSize: Int,
                          psModel: IncComFriendsPSModel, maxComFriendsNum: Int=Int.MaxValue) = {
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"incEdgePartition: partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes = new mutable.HashSet[Long]()
      pullNodes ++= batchIter.flatMap(x => Iterator(x._1, x._2))
      val beforePullTs = System.currentTimeMillis()
      val pulled = psModel.getNeighbors(pullNodes.toArray)
      println(s"incEdgePartition: partition $partId: process ${batchIter.length} edges, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      batchIter.map { case (src, dst) =>
        Row(src, dst, ArrayUtils.intersectCountWithLimits(pulled.get(src), pulled.get(dst), maxComFriendsNum))
      }
    }
  }

}
