package com.tencent.angel.graph.rank.swing

import com.tencent.angel.graph.model.neighbor.simple.SimpleNeighborTableModel
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SwingOperator {

  def userItem2NeighborTable(edges: RDD[(Long, Long)],
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

  def userItem2NeighborTable(edges: RDD[(Long, Long)],
                             partitionNum: Int,
                             itemHashSet: OpenHashSet[Long]): RDD[(Long, Array[Long])] = {
    edges.groupByKey(partitionNum).mapPartitionsWithIndex { (partId, iter) =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          val g = group.toArray.distinct.map { x => if (itemHashSet.contains(x)) x else x * -1}.sorted
          Iterator.single(src, g)
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

  def runNeighborPartition(partId: Int, iter: Iterator[(Long, Array[Long])],
                           psModel: SimpleNeighborTableModel, batchSize: Int,
                           alpha: Float, beta: Int = 0, gamma: Float = 0f,
                           superItemThreshold: Int): Iterator[(Long, Long, Float)] = {
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes = new mutable.HashSet[Long]()
      batchIter.foreach { case (_, users) => pullNodes ++= users
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborTable = psModel.getNeighbors(pullNodes.toArray)
      println(s"partition $partId: process ${batchIter.length} neighbor tables, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      computeStartTs = System.currentTimeMillis()
      batchIter.flatMap { case (src, srcNbrs) =>
        if (srcNbrs.length > 10000) {
          println(s"partition $partId: item = $src, numBoughtUsers = ${srcNbrs.length}")
        }
        val user2Items = new Array[(Long, Array[Long])](srcNbrs.length)
        var i = 0
        while (i < srcNbrs.length) {
          user2Items(i) = (srcNbrs(i), psNeighborTable.get(srcNbrs(i)))
          i += 1
        }
        val res = swing(src, user2Items, alpha, beta, gamma, superItemThreshold)
        res.toIterator
      }
    }
  }

  def calcSuperItemPairs(superItemPairs: Array[(Long, Long, Float)], batchSize: Int,
                         psModel: SimpleNeighborTableModel, partitionNum: Int,
                         superItemUsers: Long2ObjectOpenHashMap[Array[Long]],
                         alpha: Float, beta: Int, gamma: Float) = {
    val superResult = new ArrayBuffer[(Long, Long, Float)]()
    if (superItemPairs.length > 0) {
      var processed = 0
      superItemPairs.sliding(batchSize, batchSize).foreach { superBatch =>
        val before = System.currentTimeMillis()
        val tag_ = new ArrayBuffer[Int]() // to tell the current itemArray belongs to which item
      var i = 0
        val tag2len = new mutable.HashMap[Int, Int]()
        val itemsArray = new ArrayBuffer[Array[Long]]()
        val users2pull = new mutable.HashSet[Long]()
        while (i < superBatch.length) {
          val (itemI, itemJ, _) = superBatch(i)
          val usersBoughtI = superItemUsers.get(itemI) // itemI is always > 0
          users2pull ++= usersBoughtI
          i += 1
        }
        var q = 0
        val beforePull = System.currentTimeMillis()
        val usersItemTable = psModel.getNeighbors(users2pull.toArray)
        val pullTime = System.currentTimeMillis() - beforePull
        while (q < superBatch.length) {
          val (itemI, itemJ, _) = superBatch(q)
          val usersBoughtI = superItemUsers.get(itemI)
          usersBoughtI.foreach { key =>
            val items = usersItemTable.get(key)
            if (items.contains(itemJ)) {
              itemsArray += usersItemTable.get(key)
              tag_ += q
            }
          }
          tag2len.update(q, tag_.size)
          q += 1
        }
        val tag = tag_.map(t => tag2len(t)).toArray
        assert(itemsArray.length == tag.length, s"length of itemsArray and tag is not the same.")
        val assembleTime = System.currentTimeMillis() - before

        val itemsRDD = SparkContext.getOrCreate().parallelize(itemsArray.indices.toArray.zip(tag).toSeq, partitionNum)
        val score = itemsRDD.mapPartitions { iter =>
          batchRightIntersect(iter, itemsArray.toArray, alpha, beta, gamma)
        }.reduceByKey((a, b) => a + b).collect().sortWith(_._1 < _._1).map(_._2)
        superBatch.zip(score).foreach { case ((itemI, itemJ, _), s) =>
          if (itemJ < 0) {
            superResult += ((itemI, itemJ * -1, s))
          } else {
            superResult += ((itemI, itemJ, s))
            superResult += ((itemJ, itemI, s))
          }
        }
        processed += superBatch.length
        println(s"processed $processed superItemPairs, " +
          s"last batch cost ${System.currentTimeMillis() - before} ms with ${itemsArray.length} users," +
          s"assembling cost $assembleTime ms, pulling (${users2pull.size} users) cost $pullTime ms.")
      }
    }
    superResult
  }

  def swing(item_i:Long,item2items: Array[(Long,Array[Long])],alpha:Float = 0f,
            beta: Int = 0, gamma: Float = 0f, superItemThreshold: Int): Array[(Long, Long, Float)]={
    val itemSet = new mutable.HashSet[Long]()
    val itemFreq = new mutable.HashMap[Long, Int]()
    val userIndex = new mutable.HashMap[Long, Int]()
    val itemJ2Users = new mutable.HashMap[Long, mutable.HashSet[Long]]()
    var j = 0
    item2items.map { case (user, userItems) =>
      userItems.map { f =>
        if (f > item_i || f < 0) {
          itemSet.add(f)
          itemFreq.put(f, itemFreq.getOrElse(f, 0) + 1)
        }
      }
      userIndex.put(user, j)
      j += 1
    }

    val candidate = itemFreq.filter(_._2 > 1)
    item2items.map { case (user, userItems) =>
      userItems.map { f =>
        if (candidate.contains(f)) {
          if (itemJ2Users.contains(f)) {
            itemJ2Users(f).add(user)
          } else {
            val userSet = new mutable.HashSet[Long]()
            userSet.add(user)
            itemJ2Users.put(f, userSet)
          }
        }
      }
    }
    val result = new ArrayBuffer[(Long, Long, Float)]()
    var numOverThreshold = 0
    for ((item, userSet) <- itemJ2Users) {
      if (userSet.size > superItemThreshold && superItemThreshold > 0) {
        numOverThreshold += 1
        result += ((item_i, item, -1f))
      } else {
        var i = 0
        val users = userSet.toArray
        var simScore = 0f
        while (i < users.length - 1) {
          var j = i + 1
          val uItems = item2items(userIndex(users(i)))._2
          val wi = math.pow(uItems.length + beta, gamma)
          while (j < users.length) {
            val vItems = item2items(userIndex(users(j)))._2
            val wj = wi * math.pow(vItems.length + beta, gamma)
            val score = Score(uItems, vItems, alpha, wj.toFloat)
            simScore += score
            j += 1
          }
          i += 1
        }
        if (item < 0) {
          result += ((item_i, item * -1, simScore))
        } else {
          result += ((item_i, item, simScore))
          result += ((item, item_i, simScore))
        }
      }
    }
    if (numOverThreshold > 0) {
      println(s"item $item_i has $numOverThreshold items with over $superItemThreshold common users.")
    }
    result.toArray
  }

  def Score(Iu:Array[Long],Iv:Array[Long],alpha:Float, weight: Float = 1.0f): Float = {
    //    1.0f / (alpha+Iu.toSet.intersect(Iv.toSet).size)
    weight / (alpha + ArrayUtils.intersectCount(Iu, Iv))
  }

  def batchRightIntersect(users: Iterator[(Int, Int)], itemsArray: Array[Array[Long]],
                          alpha: Float, beta: Int, gamma: Float): Iterator[(Int, Float)] = {
    val Score = new mutable.HashMap[Int, Float]()
    while (users.hasNext) {
      val (user_i, tag) = users.next()
      val uItems = itemsArray(user_i)
      val wi = math.pow(uItems.length + beta, gamma)
      if (user_i < (tag - 1)) {
        val user_jArray = (user_i + 1 until tag).toArray
        for (j <- user_jArray) {
          val vItems = itemsArray(j)
          val wj = wi * math.pow(vItems.length + beta, gamma)
          val score = wj.toFloat / (alpha + ArrayUtils.intersectCount(uItems, vItems))
          Score.update(tag, Score.getOrElse(tag, 0f) + score)
        }
      }
    }
    Score.toIterator
  }

}