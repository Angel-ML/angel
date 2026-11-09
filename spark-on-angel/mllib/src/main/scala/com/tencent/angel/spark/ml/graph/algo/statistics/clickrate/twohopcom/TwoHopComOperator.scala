package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.twohopcom

import com.tencent.angel.spark.ml.graph.utils.{BatchIter, GraphIO}
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

object TwoHopComOperator {
  
  def loadUserLabelEdges(input: String,
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
      .map(row => (row.getLong(0), row.getLong(1)))
    
    edges
  }
  
  def loadUserComEdges(input: String,
                       srcNodeIndex: Int,
                       dstNodeIndex: Int,
                       srcNodeIdCol: String,
                       dstNodeIdCol: String,
                       delimiter: String): RDD[(Long, Long)] = {
    loadUserLabelEdges(input, srcNodeIndex, dstNodeIndex, srcNodeIdCol, dstNodeIdCol, delimiter)
  }
  
  def loadAdUserEdges(input: String,
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
  
  def statsFromUserCom(usercom: RDD[(Long, Long)]): (Long, Long, Long) = {
    val idSet = usercom.map(_._1)
    (idSet.min, idSet.max, idSet.count())
  }
  
  def statsFromUserEdges(nbrTable: RDD[(Long, Array[Long])]): (Long, Long, Long, Long, Int, Int) = {
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
  def runAdPartitions(iter: Iterator[(Long, Array[Long])], partitionId: Int, psModel: TwoHopComPSModel): Iterator[Row] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var batchStartTime = System.currentTimeMillis()
    
    BatchIter(iter, batchSize).flatMap{ batchIter =>
      val pullNodesOneHop = mutable.HashSet[Long]()
      val ad2Users = new Long2ObjectOpenHashMap[Array[Long]]()
      
      batchIter.foreach{ iter =>
        pullNodesOneHop ++= iter._2
        ad2Users.put(iter._1, iter._2)
      }
      
      println(s"pull ${pullNodesOneHop.size} one-hop nodes' neighbor tables from ps")
      
      val nbrTablesOneHop = psModel.getLongNeighborTable(pullNodesOneHop.toArray)
      
      val pullNodesTwoHop = mutable.HashSet[Long]()
      
      nbrTablesOneHop.keySet.toLongArray.foreach{ key =>
        pullNodesTwoHop ++= nbrTablesOneHop.get(key)
      }
      
      val nbrTablesTwoHop = psModel.getLongNeighborTable(pullNodesTwoHop.toArray)
      
      val ads = ad2Users.keySet().toLongArray()
      ads.flatMap{ ad =>
        val users = ad2Users.get(ad)
        val twoHopNbrs = mutable.HashSet[Long]()
        
        users.foreach{ user =>
          val nbrs1h = nbrTablesOneHop.get(user)
          nbrs1h.foreach{ nbr =>
            val nbrs2h = nbrTablesTwoHop.get(nbr).filter(_ != user)
            twoHopNbrs ++= nbrs2h
          }
        }
        
        val twoHopClicks = ArrayUtils.intersectCount(twoHopNbrs.toArray.sorted, users)
        val twoHopRate: Float = twoHopClicks.toFloat / twoHopNbrs.size
        
        Iterator.single(Row(ad, twoHopRate))
      }
    }
  }
  
  
}
