package com.tencent.angel.spark.ml.graph.algo.rank.commonFriendsRecall

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.algo.statistics.commonfriends.CommonFriendsPSModel
import com.tencent.angel.spark.ml.graph.utils.params._
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class CommonFollowersRecall1(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasNodeAttrPath  with HasPullBatchSize with HasTopK with HasBreakUpSave
  with HasOutputLists{

  def this() = this(Identifiable.randomUID("CommonFriendsRecall1"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges: a -> b means a follows b
    val edges = dataset.select(${srcNodeIdCol}, ${dstNodeIdCol}).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)

    edges.persist(StorageLevel.DISK_ONLY)
    println(s"----- examples of edge -----")
    println(s"${edges.take(10).map(x => x._1 + "->" + x._2).mkString(" ")}")

    // store a -> followers to ps
    val neighborPartitionsForPS = edges.map(f => (f._2, f._1)).groupByKey(${partitionNum})
      .map { case (src, neighbors) => (src, neighbors.toArray) }

    val (minId, maxId, numNodes, numEdges, maxOutDegree, minOutDegree) = CommonFollowersRecall.stats(neighborPartitionsForPS)
    println(s"----- stats of the user -> followers relations -----")
    println(s"minId: $minId, maxId: $maxId, numNodes: $numNodes, numEdges: $numEdges")
    println(s"minOutDegree: $minOutDegree, maxOutDegree: $maxOutDegree")

    // start ps and push neighborTable to ps
    println(s"----- start ps -----")
    val psStartTime = System.currentTimeMillis()
    PSContext.getOrCreate(dataset.sparkSession.sparkContext)
    println(s"starting parameter server cost ${System.currentTimeMillis() - psStartTime} ms")

    println(s"----- push neighborTable to ps -----")
    val initTableStartTime = System.currentTimeMillis()
    val psModel = CommonFriendsPSModel(maxId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum),
      minId, useBalancePartition = ${useBalancePartition})
    psModel.initLongNeighborTable(neighborPartitionsForPS, edges.flatMap(f => Iterator(f._1, f._2)))
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    // store user -> followings to worker
    val neighborPartitionsForWorker = edges.groupByKey(${partitionNum})
      .map { case (src, neighbors) => (src, neighbors.toArray) }

    val (minId_, maxId_, numNodes_, numEdges_, maxOutDegree_, minOutDegree_) = CommonFollowersRecall.stats(neighborPartitionsForWorker)
    println(s"----- stats of the user -> following relations -----")
    println(s"minId: $minId_, maxId: $maxId_, numNodes: $numNodes_, numEdges: $numEdges_")
    println(s"minOutDegree: $minOutDegree_, maxOutDegree: $maxOutDegree_")

    val result = if (${outputLists}) {
      // output the common friends lists
      val retRDD = neighborPartitionsForWorker.mapPartitionsWithIndex { case (index, iter) =>
        CommonFollowersRecall1.runNeighborPartition1(index, iter, psModel, ${topK})
      }
      val schema = StructType(Seq(
        StructField("node1", LongType, nullable = false),
        StructField("node2", LongType, nullable = false),
        StructField("comFriendsNum", IntegerType, nullable = false),
        StructField("comFriends", StringType, nullable = false)
      ))
      dataset.sparkSession.createDataFrame(retRDD.map(x => Row(x._1, x._2, x._3, x._4)), schema)
    } else {
      val result = neighborPartitionsForWorker.mapPartitionsWithIndex { case (index, iter) =>
        CommonFollowersRecall1.runNeighborPartition(index, iter, psModel, ${topK}, ${breakUpSave})
      }
      if (${breakUpSave}) {
        val retRDD = result.map {x =>
          val nodeid1 = x._1
          val recall = x._2.split(" ")
          recall.map(x => x.split(":")).map(x => (nodeid1, x(0).toLong, x(1).toLong))
        }.flatMap(x => x).map(f => Row(f._1, f._2, f._3))
        val schema = StructType(Seq(
          StructField("node1", LongType, nullable = false),
          StructField("node2", LongType, nullable = false),
          StructField("recall", LongType, nullable = false)
        ))
        dataset.sparkSession.createDataFrame(retRDD, schema)
      } else {
        val retRDD = result.map(f => Row(f._1, f._2))
        dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
      }
    }
    result
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        "recall"
      }", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object CommonFollowersRecall1 {
  def stats(neighborTable: RDD[(Long, Array[Long])]): (Long, Long, Long, Long, Long, Long) = {
    neighborTable.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      var numNodes = 0L
      var maxOutDegree = Long.MinValue
      var minOutDegree = Long.MaxValue
      iter.foreach { case(src, neighbors) =>
        maxOutDegree = math.max(maxOutDegree, neighbors.length)
        minOutDegree = math.min(minOutDegree, neighbors.length)
        min = math.min(min, math.min(neighbors.head, src))
        max = math.max(max, math.max(neighbors.last, src))
        numNodes += 1
        numEdges += neighbors.length
      }
      Iterator.single(( min, max, numNodes, numEdges, maxOutDegree, minOutDegree))
    }.reduce{ case (c1, c2) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 max c2._5, c1._6 min c2._6)
    }
  }

  def runNeighborPartition(partId: Int, iter: Iterator[(Long, Array[Long])],
                           psModel: CommonFriendsPSModel, threshold: Int = 3,
                           breakUpSave: Boolean=false): Iterator[(Long, String)] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numUsers = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNeighborTable = new Long2ObjectOpenHashMap[Array[Long]](batchSize) // user-items
      batchIter.foreach { case (user, neighbors) =>
        numUsers += 1
        localNeighborTable.put(user, neighbors)
        pullNodes ++= neighbors
        pullNodes.add(user)
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborTable = psModel.getLongNeighborTable(pullNodes.toArray)
      println(s"partition $partId: process $numUsers neighbor tables, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray()
      computeStartTs = System.currentTimeMillis()

      srcNodes.flatMap { src =>
        val srcNbrs = localNeighborTable.get(src).toSet
        val occurrence = new mutable.OpenHashMap[Long, Int]()
        srcNbrs.foreach { user =>
          psNeighborTable.get(user).foreach { i =>
            // if breakupSave, only output (a b num) where a<b, this is for isec.tesla,
            // results will be scattered before save.
            // if not, output the result in the form of recall, eg. "a  b:10 c:9 d:8"
            if (breakUpSave) {
              if (src < i) {
                val times = occurrence.getOrElse(i, 0)
                occurrence.update(i, times + 1)
              }
            } else {
              if (!srcNbrs.contains(i) && i != src) { // filter out users that have already been followed by src
                val times = occurrence.getOrElse(i, 0)
                occurrence.update(i, times + 1)
              }
            }
          }
        }
        val occ = occurrence.filter(_._2 >= threshold)
        if (occ.nonEmpty) {
          val recall = occ.toArray.sortWith(_._2 > _._2).map(x => x._1 + ":" + x._2).mkString(" ")
          Iterator.single(src, recall)
        } else Iterator.empty
      }
    }
  }

  def runNeighborPartition1(partId: Int, iter: Iterator[(Long, Array[Long])],
                           psModel: CommonFriendsPSModel, threshold: Int = 3): Iterator[(Long, Long, Int, String)] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes = new mutable.HashSet[Long]()
      batchIter.foreach { case (_, neighbors) =>
        pullNodes ++= neighbors
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborTable = psModel.getLongNeighborTable(pullNodes.toArray) // wifi-[users]
      println(s"partition $partId: process ${batchIter.length} neighbor tables, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      computeStartTs = System.currentTimeMillis()

      batchIter.flatMap { case (src, srcNbrs) => // user-[wifi]
        val occurrence = new mutable.OpenHashMap[Long, LongArrayList]()
        srcNbrs.foreach { item =>
          val dsts = psNeighborTable.get(item)
          dsts.foreach { dst =>
            if (src < dst) {
              if (!occurrence.contains(dst))
                occurrence.put(dst, new LongArrayList())
              occurrence(dst).add(item)
            }
          }
        }
       occurrence.filter(_._2.size() >= threshold).toArray.flatMap { case (dst, occ) =>
          val temp = occ.toLongArray().mkString(" ")
          Iterator((src, dst, occ.size(), temp))
        }
      }
    }
  }
}
