package com.tencent.angel.spark.ml.graph.algo.rank.biclique

import com.tencent.angel.graph.model.neighbor.complex.ComplexNeighborModel
import com.tencent.angel.spark.ml.graph.algo.statistics.commonfriends.CommonFriendsPSModel
import com.tencent.angel.spark.ml.graph.data.neighbor.NodeNeighborModel
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CooccurrenceOperator {

  def userItem2NeighborTable(edges: RDD[(Long, Long)],
                             partitionNum: Int): RDD[(Long, Array[Long])] = {
    edges.groupByKey(partitionNum).mapPartitionsWithIndex { (partId, iter) =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(src, group.toArray.sorted)
        }
      } else {
        Iterator.empty
      }
    }
  }

  def userItem2NeighborTableWithWeight(edges: RDD[(Long, (Long, Float))],
                             partitionNum: Int): RDD[(Long, Array[(Long, Float)])] = {
    edges.groupByKey(partitionNum).mapPartitionsWithIndex { (partId, iter) =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(src, group.toArray.sortBy(_._1))
        }
      } else {
        Iterator.empty
      }
    }
  }

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
        min = math.min(min, src)
        max = math.max(max, src)
        numNodes += 1
        numEdges += neighbors.length
      }
      Iterator.single(( min, max, numNodes, numEdges, maxOutDegree, minOutDegree))
    }.reduce{ case (c1, c2) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 max c2._5, c1._6 min c2._6)
    }
  }

  def getCacheNodes(partId: Int, iter: Iterator[(Long, Array[Long])], cacheRatio: Double): Iterator[Long] = {
    val re = new mutable.OpenHashMap[Long, Int]()
    iter.foreach { case (item, users) => re.update(item, users.length)}
    val shouldCacheNum = (re.size * cacheRatio).toInt
    val cache = re.toArray.sortWith(_._2 > _._2).take(shouldCacheNum)
    println(s"partition $partId, cache $shouldCacheNum item-users with num users range (${cache(0)._2}, ${cache(shouldCacheNum - 1)._2})")
    if (shouldCacheNum > 0) {
     cache.map(_._1).toIterator
    } else Iterator.empty
  }

  def getCache(itemUserIter: Iterator[(Long, Array[Long])], itemIter: Iterator[Long]): Iterator[(Long, Array[Long])] = {
    val re = new ArrayBuffer[(Long, Array[Long])]()
    val itemSet = new mutable.HashSet[Long]()
    itemIter.foreach { item => itemSet.add(item)}
    itemUserIter.foreach { case (item, users) =>
      if (itemSet.contains(item)) re += ((item, users))
    }
    re.toIterator
  }

  def runNeighborPartition(partId: Int, iter: Iterator[(Long, Array[Long])],
                           psModel: CommonFriendsPSModel): Iterator[(Long, Long, Int)] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numItems = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNeighborTable = new Long2ObjectOpenHashMap[Array[Long]](batchSize) // user-items
      batchIter.foreach { case (item, users) =>
        numItems += 1
        localNeighborTable.put(item, users)
        if (localNeighborTable.containsKey(item))
          pullNodes ++= users
      }
      val beforePullTs = System.currentTimeMillis()
      // to do: only return nbrs that is bigger than the min src
      val psNeighborTable = psModel.getLongNeighborTable(pullNodes.toArray)
      println(s"partition $partId: process $numItems neighbor tables, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray()
      computeStartTs = System.currentTimeMillis()

      srcNodes.flatMap { src =>
        val srcNbrs = localNeighborTable.get(src) //
        val occurrence = new mutable.OpenHashMap[Long, Int]()
        srcNbrs.foreach { user =>
          val relatedItems = psNeighborTable.get(user)
          relatedItems.foreach { i =>
            if (i > src) {
              val times = occurrence.getOrElse(i, 0)
              occurrence.update(i, times + 1)
            }
          }
        }
//        val occ = occurrence.filter(_._2 > 1) // occurrence time should be more than 1
        if (occurrence.nonEmpty) {
          occurrence.toArray.flatMap { case (item, times) =>
            Iterator((src, item, times), (item, src, times))
          }
        } else {
          Iterator.empty
        }
      }
    }
  }

  def runWeightedNeighborPartition(partId: Int, iter: Iterator[(Long, Array[(Long, Float)])],
                                   model: ComplexNeighborModel, batchSize: Int): Iterator[(Long, Long, Float)] = {
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numItems = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNeighborTable = new Long2ObjectOpenHashMap[Array[(Long, Float)]](batchSize) // user-items
      batchIter.foreach { case (item, users) =>
        numItems += 1
        localNeighborTable.put(item, users)
        if (localNeighborTable.containsKey(item))
          pullNodes ++= users.map(_._1)
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborTable = model.getNeighbors(pullNodes.toArray)
      println(s"partition $partId: process $numItems neighbor tables, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcItems = localNeighborTable.keySet().toLongArray()
      computeStartTs = System.currentTimeMillis()

      srcItems.flatMap { srcItem =>
        val neiUsers = localNeighborTable.get(srcItem) //
        val userWeightMap = new mutable.OpenHashMap[Long, Float]()
        neiUsers.foreach { case (user, weight) => userWeightMap.update(user, weight)}
        val occurrence = new mutable.OpenHashMap[Long, ArrayBuffer[(Long, Float)]]() // item -> Array[(user, weight)]
        neiUsers.foreach { case (user, weight) =>
          val relatedItems = psNeighborTable.get(user).getNeighborIds
          val relatedWeights = psNeighborTable.get(user).getAttrs
          relatedItems.zip(relatedWeights).foreach { case (item, weight) =>
            if (item > srcItem) {
              occurrence.update(item, occurrence.getOrElse(item, new ArrayBuffer[(Long, Float)]()) += ((user, weight)))
            }
          }
        }
        val occ = occurrence.filter(_._2.length > 1)
        if (occ.nonEmpty) {
          occ.toArray.flatMap { case (item, userWeights) =>
            var occScore = 0d
            var i = 0
            while (i < userWeights.length - 1) {
              var j = i + 1
              while (j < userWeights.length) {
                val geometricAve = math.pow(userWeights(i)._2 * userWeights(j)._2 * userWeightMap(userWeights(i)._1) * userWeightMap(userWeights(j)._1), 0.25)
                occScore += geometricAve
                j += 1
              }
              i += 1
            }
            Iterator((srcItem, item, occScore.toFloat), (item, srcItem, occScore.toFloat))
          }
        } else {
          Iterator.empty
        }
      }
    }
  }

}