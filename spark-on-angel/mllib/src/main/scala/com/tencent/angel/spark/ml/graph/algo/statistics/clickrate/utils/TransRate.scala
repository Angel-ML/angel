package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.utils

import com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehop.OneHopPSModel
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object TransRate {
  def onehopTransRate(dt1: RDD[(Long, Array[Long])], dt2: RDD[(Long, Array[Long])], psModel: OneHopPSModel, batchSize: Int): RDD[(Long, Float)] = {
    val data = dt1.join(dt2)
    data.mapPartitions{ iter =>
      BatchIter(iter, batchSize).flatMap{ batchIter =>
        val pullNodes = mutable.HashSet[Long]()
        val incSet = mutable.HashSet[Long]()
        batchIter.foreach{ tb =>
          pullNodes ++= tb._2._1
          incSet ++= tb._2._2
        }
        
        val nbrTable = psModel.getLongNeighborTable(pullNodes.toArray)

        batchIter.flatMap{ tb =>
          val newClicks = tb._2._2.toSet -- tb._2._1.toSet
          val onehopSet = mutable.HashSet[Long]()
          tb._2._1.foreach{ node =>
            onehopSet ++= nbrTable.get(node)
          }
          
          var inOneHopCnt = 0
          var allCnt = 0
          newClicks.foreach { nc =>
            allCnt += 1
            if (onehopSet.contains(nc)) {
              inOneHopCnt += 1
            }
          }
          val tsR = inOneHopCnt.toFloat / allCnt.toFloat
          Iterator.single((tb._1, tsR))
        }
      }
    }
  }
  
}
