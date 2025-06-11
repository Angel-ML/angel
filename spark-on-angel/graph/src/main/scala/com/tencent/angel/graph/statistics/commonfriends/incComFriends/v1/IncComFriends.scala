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
package com.tencent.angel.graph.statistics.commonfriends.incComFriends.v1

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
import scala.collection.mutable.ArrayBuffer

class IncComFriends(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasCompressCol
  with HasIsCompressed with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasCompressIndex
  with HasInput with HasExtraInputs with HasDelimiter with HasUseBalancePartition
  with HasMinNodeId with HasMaxNodeId with HasSetCheckPoint {

  def this() = this(Identifiable.randomUID("IncCommonFriends"))

  private var approxNumNodes: Long = -1L
  def setNumNodes(in: Long): Unit = { this.approxNumNodes = in }

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

    val min = $(minNodeId)
    val max = $(maxNodeId)

    val tagged = incEdges.flatMap(x => Iterator(x._1, x._2))
    val minId = math.min(tagged.min(), min)
    val maxId = math.max(tagged.max(), max)

    //    val taggedNodes = incEdges.map(_._1).distinct().persist(StorageLevel.MEMORY_ONLY)
    //    println(s"num tagged nodes: $(taggedNodes.count()}")
    val taggedNodes = incEdges.map(_._1)

    println(s"====== start parameter server ======")
    val psStartTime = System.currentTimeMillis()
    startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

//    val nodeNum = firstEdges.flatMap(x => Iterator(x._1, x._2)).countApproxDistinct()
//    println(s"approx distinct node num=$nodeNum")
    val nbrModelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, approxNumNodes,
      "neighbor", sc.hadoopConfiguration)
    val tagModelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "tag", sc.hadoopConfiguration)
    val psModel = new IncComFriendsPSModel(nbrModelContext, tagModelContext)
    psModel.init()

    val tagNum = psModel.updateTag(taggedNodes)
    println(s"init $tagNum nodes.")
    //    val time = System.currentTimeMillis()
    //    psModel.checkpointTag()
    //    println(s"checkpoint tag cost $(System.currentTimeMillis() - time} ms.")

    // push neighbor table to ps
    println(s"====== load edges and push neighborTable to ps ======")
    val firstEdges: RDD[(Long, Long)] = IncComFriends.loadEdges(dataset, $(partitionNum))
    firstEdges.unpersist()
    psModel.initNbrTable(firstEdges, $(batchSize))

    val incNbrs = incEdges.groupByKey().map(x => (x._1, x._2.toArray)).persist($(storageLevel))
    psModel.initIncNbrTable(incNbrs, 2000)

    val transStartTime = System.currentTimeMillis()
    psModel.trans()
    println(s"trans cost ${System.currentTimeMillis() - transStartTime} ms.")
    val ckpStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint cost ${System.currentTimeMillis() - ckpStartTime} ms.")

    // filter and save the unchanged edges, no deleted edges
    var beforeCalc = System.currentTimeMillis()
    val firstComFriends = IncComFriends.loadComFriendsResults(dataset, $(partitionNum))
    firstComFriends.unpersist()
    val partRe = firstComFriends.mapPartitionsWithIndex { case (index, iter) =>
      BatchIter(iter, $(pullBatchSize)).flatMap { batchIter =>
        val startTime = System.currentTimeMillis()
        val pullNodes = new mutable.HashSet[Long]()
        pullNodes ++= batchIter.flatMap(x => Iterator(x._1, x._2))
        var beforePull = System.currentTimeMillis()
        val tags = psModel.readTag(pullNodes.toArray)
        val pullTagTime = System.currentTimeMillis() - beforePull

        val toSave = new ArrayBuffer[Row]()
        val toCalc = new ArrayBuffer[(Long, Long)]()
        pullNodes.clear()

        batchIter.foreach { case (src, dst, num) =>
          if (tags.get(src) == 1 || tags.get(dst) == 1) {
            toCalc.append((src, dst))
            pullNodes.add(src)
            pullNodes.add(dst)
          } else toSave.append(Row(src, dst, num))
        }
        var pullNbrTime = 0L
        if (pullNodes.nonEmpty) {
          beforePull = System.currentTimeMillis()
          val pulledNbrs = psModel.getNbrTable(pullNodes.toArray)
          pullNbrTime = System.currentTimeMillis() - beforePull
          toCalc.toIterator.foreach { case(src, dst) =>
            val t = ArrayUtils.intersectCountWithLimits(pulledNbrs.get(src), pulledNbrs.get(dst), maxComFriendsNum)
            toSave.append(Row(src, dst, t))
          }
        }
        println(s"distinguish edges: partition $index, pull ${tags.size} nodes' tag, cost $pullTagTime ms, " +
          s"pull ${pullNodes.size} nodes' nbrs, cost $pullNbrTime ms, last batch cost ${System.currentTimeMillis() - startTime} ms.")
        toSave.toIterator
      }
    }
    partRe.unpersist()

    val schema = StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(dstNodeIdCol), LongType, nullable = false),
      StructField($(numCommonFriendsCol), IntegerType, nullable = false)
    ))
    GraphIO.save(dataset.sparkSession.createDataFrame(partRe, schema), outputPath)
    println(s"calc1 cost ${System.currentTimeMillis() - beforeCalc} ms.")

    beforeCalc = System.currentTimeMillis()
    val partRe1 = incNbrs.mapPartitionsWithIndex { case (index, iter) =>
      IncComFriends.runIncNbrPartition(index, iter, 2000, psModel, maxComFriendsNum)
    }
    GraphIO.appendSave(dataset.sparkSession.createDataFrame(partRe1, schema), outputPath)
    println(s"calc2 cost ${System.currentTimeMillis() - beforeCalc} ms.")
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

  def loadEdges(dataset: Dataset[_], partitionNum: Int): RDD[(Long, Long)] = {
    dataset.select("src", "dst").rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        Iterator.single(row.getLong(0), row.getLong(1))
      }
    }.coalesce(partitionNum)
  }

  def loadComFriendsResults(dataset: Dataset[_], partitionNum: Int): RDD[(Long, Long, Int)] = {
    dataset.select("src", "dst", "weight").rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        Iterator.single(row.getLong(0), row.getLong(1), row.getFloat(2).toInt)
      }
    }.coalesce(partitionNum)
  }

  def runIncNbrPartition(partId: Int, iter: Iterator[(Long, Array[Long])], batchSize: Int,
                         psModel: IncComFriendsPSModel, maxComFriendsNum: Int=Int.MaxValue) = {
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"incEdgePartition: partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes = new mutable.HashSet[Long]()
      batchIter.foreach { case (src, nbrs) =>
        pullNodes.add(src)
        pullNodes ++= nbrs
      }
      val beforePullTs = System.currentTimeMillis()
      val pulled = psModel.getNbrTable(pullNodes.toArray)
      println(s"incEdgePartition: partition $partId: process ${batchIter.length} edges, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      batchIter.flatMap { case (src, nbrs) =>
        nbrs.map { dst =>
          Row(src, dst, ArrayUtils.intersectCountWithLimits(pulled.get(src), pulled.get(dst), maxComFriendsNum))
        }
      }
    }
  }

}
