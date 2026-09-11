package com.tencent.angel.graph.community.egonetwork

import com.tencent.angel.graph.statistics.commonfriends.CommonFriendsPSModel
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.graph.community.egonetwork.EgoNetworkV2.{find, intersect}
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, Long2ObjectOpenHashMap, LongArrayList}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object EgoOperator {

  def edge2NeighborTable(edges: RDD[(Long, Long)],
                             partitionNum: Int): RDD[(Long, Array[Long])] = {
    edges.groupByKey(partitionNum).mapPartitionsWithIndex { (partId, iter) =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(src, group.toArray.distinct.sorted)
        }
      } else {
        Iterator.empty
      }
    }
  }

  def runNeighborPartition(partId: Int, iter: Iterator[(Long, Array[Long])],
                           psModel: CommonFriendsPSModel,
                           compressEdges: Boolean=false): Iterator[(Long, String, String)] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numItems = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNeighborTable = new Long2ObjectOpenHashMap[Array[Long]](batchSize)
      batchIter.foreach { case (item, users) =>
        numItems += 1
          localNeighborTable.put(item, users)
          if (localNeighborTable.containsKey(item))
            pullNodes ++= users
        //        pullNodes.add()
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborTable = psModel.getLongNeighborTable(pullNodes.toArray)
      println(s"partition $partId: process $numItems neighbor tables, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val validIndices = new Long2IntOpenHashMap(psNeighborTable.size())
      val t = psNeighborTable.long2ObjectEntrySet().fastIterator()
      val beforeSplit = System.currentTimeMillis()
      while (t.hasNext) {
        val data = t.next()
        val node = data.getLongKey
        val neis = data.getValue
        validIndices.put(node, find(neis, node))
      }

      val srcNodes = localNeighborTable.keySet().toLongArray()
      computeStartTs = System.currentTimeMillis()

      srcNodes.map { src =>
        val srcNbrs = localNeighborTable.get(src) // users bought src
        val res = new Long2ObjectOpenHashMap[LongArrayList]()
        srcNbrs.foreach { nei =>
          intersect(srcNbrs, psNeighborTable.get(nei), validIndices.get(nei), nei, res)
        }
        val re = if (compressEdges)
          res.keySet().toLongArray().map(x => x + ":" + res.get(x).toLongArray().mkString(",")).mkString(" ")
        else
          res.keySet().toLongArray().map(x => res.get(x).toLongArray().map(i => x + ":" + i).mkString(" ")).mkString(" ")
        (src, srcNbrs.mkString(":"), re)
      }
    }
  }

  def runNodePartition(partId: Int, iter: Iterator[Long],
                       psModel: CommonFriendsPSModel,
                       compressEdges: Boolean=false): Iterator[(Long, String, String)] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      val pullSrcNodes = new mutable.HashSet[Long]()
      batchIter.foreach { node =>
        pullSrcNodes.add(node)
      }
      var beforePullTs = System.currentTimeMillis()
      val localNeighborTable = psModel.getLongNeighborTable(pullSrcNodes.toArray)
      val firstPullTime = System.currentTimeMillis() - beforePullTs

      val pullNodes = new mutable.HashSet[Long]()
      val srcNodes = localNeighborTable.keySet().toLongArray()
      srcNodes.foreach { node =>
        pullNodes ++= localNeighborTable.get(node)
      }
      beforePullTs = System.currentTimeMillis()
      val psNeighborTable = psModel.getLongNeighborTable(pullNodes.toArray)
      println(s"partition $partId: process ${pullSrcNodes.size} nodes, " +
        s"firstly pull ${pullSrcNodes.size} nodes, cost $firstPullTime ms, " +
        s"secondly pull ${pullNodes.size} nodes, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val validIndices = new Long2IntOpenHashMap(psNeighborTable.size())
      val t = psNeighborTable.long2ObjectEntrySet().fastIterator()
      val beforeSplit = System.currentTimeMillis()
      while (t.hasNext) {
        val data = t.next()
        val node = data.getLongKey
        val neis = data.getValue
        validIndices.put(node, find(neis, node))
      }

      computeStartTs = System.currentTimeMillis()

      srcNodes.map { src =>
        val srcNbrs = localNeighborTable.get(src) // users bought src

        val res = new Long2ObjectOpenHashMap[LongArrayList]()
        srcNbrs.foreach { nei =>
          intersect(srcNbrs, psNeighborTable.get(nei), validIndices.get(nei), nei, res)
        }
        val re = if (compressEdges)
          res.keySet().toLongArray().map(x => x + ":" + res.get(x).toLongArray().mkString(",")).mkString(" ")
        else
          res.keySet().toLongArray().map(x => res.get(x).toLongArray().map(i => x + ":" + i).mkString(" ")).mkString(" ")
        (src, srcNbrs.mkString(":"), re)
      }
    }
  }
}
