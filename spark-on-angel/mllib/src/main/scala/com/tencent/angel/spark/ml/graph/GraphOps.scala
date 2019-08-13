package com.tencent.angel.spark.ml.graph

import com.tencent.angel.spark.ml.graph.data.{GraphStats, NeighborTable, NeighborTablePartition, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

object GraphOps {

  def loadEdges(dataset: Dataset[_],
                srcNodeIdCol: String,
                dstNodeIdCol: String
               ): RDD[(VertexId, VertexId)] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol).rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (row.getLong(0) == row.getLong(1))
          Iterator.empty
        else
          Iterator.single((row.getLong(0), row.getLong(1)))
      }
    }
  }

  def loadEdgesWithAttr[@specialized(
    Byte, Boolean, Short, Int, Long, Float, Double) ED: ClassTag](dataset: Dataset[_],
                                                                  srcNodeIdCol: String,
                                                                  dstNodeIdCol: String,
                                                                  attrCol: String): RDD[(VertexId, (VertexId, ED))] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol, attrCol).rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (row.getLong(0) == row.getLong(1))
          Iterator.empty
        else {
          val attr = row.get(2).asInstanceOf[ED]
          Iterator.single((row.getLong(0), (row.getLong(1), attr)))
        }
      }
    }
  }

  def edgesToNeighborTable(edges: RDD[(Long, Long)],
                           partitionNum: Int): RDD[NeighborTable[Object]] = {
    edges.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(NeighborTable(
            src,
            group.toArray.filter(_ != src).sorted))
        }
      } else {
        Iterator.empty
      }
    }
  }

  def buildNeighborTablePartition[ED: ClassTag](data: RDD[NeighborTable[ED]],
                                                isDirected: Boolean = false): RDD[NeighborTablePartition[ED]] = {
    NeighborTablePartition.fromNeighborTableRDD(data, isDirected)
  }

  def getStats[ED: ClassTag](data: RDD[NeighborTablePartition[ED]]): GraphStats = {
    data.map(_.stats).reduce(_ + _)
  }
}