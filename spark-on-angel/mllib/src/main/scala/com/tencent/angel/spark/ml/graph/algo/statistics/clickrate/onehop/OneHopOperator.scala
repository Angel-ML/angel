package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehop

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.{BatchIter, GraphIO}
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OneHopOperator {
  def loadUserEdges(dataset: Dataset[_],
                    srcNodeIdCol: String,
                    dstNodeIdCol: String,
                    needReplicaEdge: Boolean = false): RDD[(Long, Long)] = {
    val edges = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    
    if (needReplicaEdge) {
      edges.flatMap{ case (src, dst) => Iterator((src, dst), (dst, src)) }
    }
    else {
      edges
    }
  }
  
  def loadUserAdEdges(input: String,
                      srcNodeIndex: Int,
                      dstNodeIndex: Int,
                      srcNodeIdCol: String,
                      dstNodeIdCol: String,
                      delimiter: String): RDD[(Long, Long)] = {
    val dataframe = GraphIO.load(input,
      isWeighted = false,
      srcIndex = srcNodeIndex,
      dstIndex = dstNodeIndex,
      sep = delimiter)
    
    val edges = dataframe.select(srcNodeIdCol, dstNodeIdCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(1), row.getLong(0)))
      
    edges
  }
  
  def edges2NeighborTable(edges: RDD[(Long, Long)], partitionNum: Int, needReplicaEdge: Boolean): RDD[(Long, Array[Long])] = {
    if (needReplicaEdge) {
      edges.flatMap(e => Iterator((e._1, e._2), (e._2, e._1)))
        .groupByKey(partitionNum).map{ nbrTable =>
        (nbrTable._1, nbrTable._2.filter(_ != nbrTable._1).toArray.distinct.sorted)
      }
    }
    else {
      edges.groupByKey(partitionNum).map{ nbrTable =>
        (nbrTable._1, nbrTable._2.filter(_ != nbrTable._1).toArray.distinct.sorted)
      }
    }
  }
  
  def makeAdUserList(edges: RDD[(Long, Long)], partitionNum: Int): RDD[(Long, Array[Long])] = {
    edges.groupByKey(partitionNum).map{ adUsers =>
      (adUsers._1, adUsers._2.toArray.distinct)
    }
  }
  
  def calculateStats(nbrTable: RDD[(Long, Array[Long])]): (Long, Long, Long, Long, Int, Int) = {
    nbrTable.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      var numNodes = 0L
      var maxDegree = 0
      var minDegree = Int.MaxValue
      iter.foreach { case(src, neighbors) =>
        min = math.min(min, math.min(src, neighbors.head))
        max = math.max(max, math.max(src, neighbors.last))
        numNodes += 1
        numEdges += neighbors.length
        maxDegree = math.max(maxDegree, neighbors.length)
        minDegree = math.min(minDegree, neighbors.length)
      }
      Iterator.single((min, max, numNodes, numEdges, minDegree, maxDegree))
    }.reduce{ case (c1: (Long, Long, Long, Long, Int, Int), c2: (Long, Long, Long, Long, Int, Int)) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 min c2._5, c1._6 max c2._6)
    }
  }
  
  //todo: may cause monster nodes problem
  def runAdPartitions(iter: Iterator[(Long, Array[Long])], partitionId: Int, psModel: OneHopPSModel): Iterator[Row] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var batchStartTime = System.currentTimeMillis()
    
    BatchIter(iter, batchSize).flatMap{ batchIter =>
      val pullNodes = mutable.HashSet[Long]()
      val ad2Users = new Long2ObjectOpenHashMap[Array[Long]]()
      
      batchIter.foreach{ iter =>
        pullNodes ++= iter._2
        ad2Users.put(iter._1, iter._2)
      }
      
      println(s"pull ${pullNodes.size} nodes' neighbor tables from ps")
      
      val nbrTables = psModel.getLongNeighborTable(pullNodes.toArray)
      val ads = ad2Users.keySet().toLongArray()
      ads.flatMap{ ad =>
        val users = ad2Users.get(ad)
        
        val oneHopNbrs = mutable.HashSet[Long]()
        users.foreach{ user =>
          oneHopNbrs ++= nbrTables.get(user)
        }
        
        val oneHopClicks = ArrayUtils.intersectCount(oneHopNbrs.toArray.sorted, users)
        val oneHopRate: Float = oneHopClicks.toFloat / oneHopNbrs.size
        
        Iterator.single(Row(ad, oneHopRate))
      }
    }
  }
  
  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }
  
  def testPS(neighborsRDD: RDD[(Long, Array[Long])], psModel: OneHopPSModel, num: Int): Boolean = {
    println(s"======check correctness======")
    val sampled = neighborsRDD.takeSample(false, num)
    var correct = true
    for (i <- 0 until sampled.length - 1) {
      val item1 = sampled(i)
      val item2 = sampled(i+1)
      val trueNum = ArrayUtils.intersectCount(item1._2, item2._2)
      val fromPS = psModel.getLongNeighborTable(Array(item1._1, item2._1))
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
