package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.lbsbased

import com.tencent.angel.spark.ml.graph.utils.{BatchIter, GraphIO}
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, Long2ObjectOpenHashMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

object LBSBasedOperator {
  
  def loadLBSUserEdges(input: String,
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
  
  def runLBSPartitions(iter: Iterator[(Long, Array[Long])], partitionId: Int, psModel: LBSBasedPSModel): Iterator[Row] = {
    val batchSize = psModel.userAdsTable.param.pullBatchSize
    var batchStartTime = System.currentTimeMillis()
    
    BatchIter(iter, batchSize).flatMap{ LBSUsers =>
      val pullNodes = mutable.HashSet[Long]()
      LBSUsers.foreach{ lu =>
        pullNodes ++= lu._2
      }
      
      println(s"pull ${pullNodes.size} userAds tables from ps")
      
      val userAds = psModel.getLongAdTable(pullNodes.toArray)
      LBSUsers.flatMap{ lbsu =>
        val adsCnt: Long2IntOpenHashMap = new Long2IntOpenHashMap()
        lbsu._2.foreach{ usr =>
          val ads = userAds.get(usr)
          ads.foreach{ ad =>
            adsCnt.addTo(ad, 1)
          }
        }
        
        val adsArray = adsCnt.keySet().toLongArray()
        
        adsArray.flatMap{ ad =>
          val clickR = adsCnt.get(ad).toFloat / lbsu._2.length
          Iterator.single(Row(lbsu._1, ad, clickR))
        }
        
      }.toSeq
    }
  }
  
}
