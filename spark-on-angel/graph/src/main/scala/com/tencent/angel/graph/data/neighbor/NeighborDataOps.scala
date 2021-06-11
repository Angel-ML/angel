package com.tencent.angel.graph.data.neighbor

import com.tencent.angel.graph.model.neighbor.simple.SimpleNeighborTableModel
import com.tencent.angel.utils.ArrayUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object NeighborDataOps {
  def loadEdges(dataset: Dataset[_],
                srcNodeIdCol: String,
                dstNodeIdCol: String,
                needReplicateEdges: Boolean = false,
                dropSelfLoopEdges: Boolean = true
               ): RDD[(Long, Long)] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol).rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (dropSelfLoopEdges && row.getLong(0) == row.getLong(1))
          Iterator.empty
        else {
          if (needReplicateEdges)
            Iterator((row.getLong(0), row.getLong(1)), (row.getLong(1), row.getLong(0)))
          else
            Iterator.single(row.getLong(0), row.getLong(1))
        }
      }
    }
  }

  def loadCompressedEdges(dataset: Dataset[_],
                          srcNodeIdCol: String,
                          dstNodeIdCol: String,
                          compressCol: String
                         ): RDD[(Long, Long)] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol, compressCol).rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (row.getLong(0) == row.getLong(1))
          Iterator.empty
        else if (row.getFloat(2) == 1)
          Iterator((row.getLong(0), row.getLong(1)), (row.getLong(1), row.getLong(0)))
        else
          Iterator.single(row.getLong(0), row.getLong(1))
      }
    }
  }

  def edges2NeighborTable(edges: RDD[(Long, Long)],
                          partitionNum: Int): RDD[(Long, Array[Long])] = {
    edges.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(src, group.toArray.filter(_ != src).distinct.sorted)
        }
      } else {
        Iterator.empty
      }
    }
  }

  def statsByNeighborTable(neighborTable: RDD[(Long, Array[Long])]): (Long, Long, Long, Long, Int, Int) = {
    neighborTable.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      var numNodes = 0L
      var maxDegree = 0
      var minDegree = Int.MaxValue
      iter.foreach { case (src, neighbors) =>
        min = math.min(min, math.min(src, neighbors.head))
        max = math.max(max, math.max(src, neighbors.last))
        numNodes += 1
        numEdges += neighbors.length
        maxDegree = math.max(maxDegree, neighbors.length)
        minDegree = math.min(minDegree, neighbors.length)
      }
      Iterator.single((min, max, numNodes, numEdges, minDegree, maxDegree))
    }.reduce { case (c1: (Long, Long, Long, Long, Int, Int), c2: (Long, Long, Long, Long, Int, Int)) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 min c2._5, c1._6 max c2._6)
    }
  }

  def testPS(neighborsRDD: RDD[(Long, Array[Long])], model: SimpleNeighborTableModel, num: Int): Unit = {
    println(s"======test PS======")
    val nodeIds = neighborsRDD.take(num).map(_._1)
    val nodeNeighbors = model.getNeighbors(nodeIds)
    val iter = nodeNeighbors.long2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      println(s"node id = ${entry.getLongKey}, neighbors = ${entry.getValue.mkString(",")}")
    }
  }

  def checkValid(neighborsRDD: RDD[(Long, Array[Long])], model: SimpleNeighborTableModel, num: Int): Boolean = {
    println(s"======check correctness======")
    val sampled = neighborsRDD.takeSample(false, num)
    var correct = true
    for (i <- 0 until sampled.length - 1) {
      val item1 = sampled(i)
      val item2 = sampled(i + 1)
      val trueNum = ArrayUtils.intersectCount(item1._2, item2._2)
      val fromPS = model.getNeighbors(Array(item1._1, item2._1))
      val psNum = ArrayUtils.intersectCount(fromPS.get(item1._1), fromPS.get(item2._1))
      println(s"friends of ${item1._1} = ${item1._2.length} [RDD] ${fromPS.get(item1._1).length} [PS], " +
        s"friends of ${item2._1}: ${item2._2.length} [RDD] ${fromPS.get(item2._1).length} [PS], " +
        s"common friends = $trueNum [RDD] $psNum [PS]")
      if (correct && trueNum != psNum)
        correct = false
    }
    correct
  }
}
