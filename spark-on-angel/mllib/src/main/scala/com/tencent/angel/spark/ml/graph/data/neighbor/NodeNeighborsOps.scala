package com.tencent.angel.spark.ml.graph.data.neighbor

import com.tencent.angel.graph.model.neighbor.complex.{ComplexNeighborModel, ComplexNeighborUtils}
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.ml.graph.algo.statistics.triangle.MotifNeighborModel
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrTagElement
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class NodeNeighborsOps(model: ComplexNeighborModel) extends Serializable {

  def initLongNeighborAttrTag(data: RDD[NodeNeighborPartition[Float]], batchSize: Int): Unit = {
    data.foreach { part =>
      val size = part.size
      var pos = 0
      while (pos < size) {
        initLongNeighborAttrTag(part.takeBatch(pos, batchSize))
        pos += batchSize
      }
    }
  }

  def initLongNeighborAttrTag(pairs: Array[NodeNeighbors[Float]]): Unit = {
    val nodeIds = new Array[Long](pairs.length)
    val neighbors = new Array[IElement](pairs.length)

    pairs.zipWithIndex.foreach(e => {
      nodeIds(e._2) = e._1.srcId
      neighbors(e._2) = new NeighborsAttrTagElement(e._1.neighbors, e._1.tags, e._1.attrs)
    })

    model.initNeighbors(nodeIds, neighbors)
  }

  def calWeightedMotifV2[ED: ClassTag](neighborsRDD: RDD[NodeNeighborPartition[ED]],
                                       batchSize: Int, isWeighted: Boolean): RDD[((Long, Byte), Float)] = {
    neighborsRDD.flatMap(_.calMotifDirectedV2(model, batchSize, isWeighted))
  }

  def calWeightedGroupMotifV2[ED: ClassTag](neighborsRDD: RDD[NodeNeighborPartition[ED]],
                                       batchSize: Int, isWeighted: Boolean): RDD[((Long, Byte), Float)] = {
    neighborsRDD.flatMap(_.calGroupMotifDirectedV2(model.asInstanceOf[MotifNeighborModel], batchSize, isWeighted))
  }

  // mark super nodes
  def calWeightedMotifV3[ED: ClassTag](neighborsRDD: RDD[NodeNeighborPartition[ED]], threshold: Int, batchSize: Int,
                                       isWeighted: Boolean): RDD[((Long, Byte), Float)] = {
    neighborsRDD.flatMap(_.calMotifDirectedV2(model, batchSize, isWeighted, threshold))
  }

}
