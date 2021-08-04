package com.tencent.angel.graph.statistics.motif

import com.tencent.angel.graph.model.neighbor.complex.{ComplexNeighborModel, ComplexNeighborUtils, NeighborsAttrTagElement}
import com.tencent.angel.graph.utils.element.{NeighborTable, NeighborTablePartition}
import com.tencent.angel.ps.storage.vector.element.IElement
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MotifNeighborOps(model: ComplexNeighborModel) extends Serializable {

  def initLongNeighborAttrTag(data: RDD[NeighborTablePartition[Float]], batchSize: Int): Unit = {
    data.foreach { part =>
      val size = part.size
      var pos = 0
      while (pos < size) {
        initLongNeighborAttrTag(part.takeBatch(pos, batchSize))
        pos += batchSize
      }
    }
  }

  def initLongNeighborAttrTag(pairs: Array[NeighborTable[Float]]): Unit = {
    val nodeIds = new Array[Long](pairs.length)
    val neighbors = new Array[IElement](pairs.length)

    pairs.zipWithIndex.foreach(e => {
      nodeIds(e._2) = e._1.srcId
      neighbors(e._2) = new NeighborsAttrTagElement(e._1.neighborIds, e._1.tags, e._1.attrs)
    })

    model.initNeighbors(nodeIds, neighbors)
  }

  def calWeightedMotif[ED: ClassTag](neighborsRDD: RDD[NeighborTablePartition[ED]],
                                     batchSize: Int, isWeighted: Boolean): RDD[((Long, Byte), Float)] = {
    neighborsRDD.flatMap(_.calMotifDirected(model, batchSize, isWeighted))
  }

}
