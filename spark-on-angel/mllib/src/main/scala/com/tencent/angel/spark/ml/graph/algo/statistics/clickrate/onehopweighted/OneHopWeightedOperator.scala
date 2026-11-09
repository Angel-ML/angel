package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehopweighted

import com.tencent.angel.spark.ml.graph.utils.{BatchIter, GraphIO}
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.InitNeighborsAttrs.NeighborsAttrsElement
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

object OneHopWeightedOperator {
  def initPSWithNbrAttr(psModel: OneHopWeightedPSModel, data: Array[(Long, Iterable[(Long, Float)])]): Unit = {
    val nodeIdToNbrAttr = new Long2ObjectOpenHashMap[NeighborsAttrsElement](data.length)
    for (info <- data) {
      val (nbrs, attrs) = info._2.toArray.unzip
      nodeIdToNbrAttr.put(info._1, new NeighborsAttrsElement(nbrs, attrs))
    }
    psModel.initNbrAttrMsgs(nodeIdToNbrAttr)
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
  
  def runAdPartitions(iter: Iterator[(Long, Array[Long])], partitionId: Int, psModel: OneHopWeightedPSModel, batchSize: Int): Iterator[Row] = {
    var batchStartTime = System.currentTimeMillis()
    
    BatchIter(iter, batchSize).flatMap{ batchIter =>
      val pullNodes = mutable.HashSet[Long]()
      val ad2Users = new Long2ObjectOpenHashMap[Array[Long]]()
      
      batchIter.foreach{ iter =>
        pullNodes ++= iter._2
        ad2Users.put(iter._1, iter._2)
      }
      
      println(s"pull ${pullNodes.size} nodes' neighbor tables from ps")
      
      val nbrAttrTables = psModel.getNbrAttrMsgs(pullNodes.toArray)
      val ads = ad2Users.keySet().toLongArray()
      ads.flatMap{ ad =>
        val users = ad2Users.get(ad)
        val userSet = users.toSet
        
        val oneHopNbrAttrs = mutable.HashSet[(Long, Long, Float)]()
        var weightCntAll = 0f
        var weightCntAd = 0f
        
        users.foreach{ user =>
          val nbrAttrs= nbrAttrTables.get(user)
          val naArray = nbrAttrs.getNeighborIds.zip(nbrAttrs.getAttrs)

          naArray.foreach { na =>
            val (srcN, dstN, w) = if (user < na._1) (user, na._1, na._2) else (na._1, user, na._2)
            if (!oneHopNbrAttrs.contains((srcN, dstN, w))) {
              oneHopNbrAttrs.add(srcN, dstN, na._2)
              weightCntAll += w
              if (userSet.contains(srcN) && userSet.contains(dstN)) {
                weightCntAd += w
              }
            }
          }
        }
        
        val oneHopWeightedRate = weightCntAd / weightCntAll
        Iterator.single(Row(ad, oneHopWeightedRate))
      }
    }
  }
  
}
