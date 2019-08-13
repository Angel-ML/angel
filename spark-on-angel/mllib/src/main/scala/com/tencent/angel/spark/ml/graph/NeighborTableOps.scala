package com.tencent.angel.spark.ml.graph

import com.tencent.angel.graph.client.initneighbor2.{InitNeighbor => InitLongNeighbor, InitNeighborParam => InitLongNeighborParam}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.LongArrayElement
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.data.{LinkPredictionMetric, NeighborTable, NeighborTablePartition}
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class NeighborTableOps(table: NeighborTableModel) extends Serializable {

  def initLongNeighbor[ED: ClassTag](data: RDD[NeighborTablePartition[ED]]): NeighborTableModel = {
    // Neighbor table : a (1, maxIndex + 1) dimension matrix
    val mc: MatrixContext = new MatrixContext()
    mc.setName(table.neighborTableName)
    mc.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    mc.setRowNum(1)
    mc.setColNum(table.param.maxIndex)
    mc.setMaxColNumInBlock(table.param.maxIndex / table.param.psPartNum)
    mc.setValueType(classOf[LongArrayElement])
    table.psMatrix = PSMatrix.matrix(mc)

    data.map { part =>
      val size = part.size
      var pos = 0
      while (pos < size) {
        initLongNeighbor(table.psMatrix, part.takeBatch(pos, table.param.batchSize))
        pos += table.param.batchSize
      }
    }.collect()
    table
  }

  def initLongNeighbor[ED: ClassTag](psMatrix: PSMatrix, pairs: Array[NeighborTable[ED]]): NeighborTableModel = {
    val nodeIdToNeighbors = new Long2ObjectOpenHashMap[Array[Long]](pairs.length)
    pairs.foreach { item =>
      require(item.srcId < table.param.maxIndex, s"${item.srcId} exceeds the maximal node index ${table.param.maxIndex}")
      nodeIdToNeighbors.put(item.srcId, item.neighborIds)
    }
    val func = new InitLongNeighbor(new InitLongNeighborParam(psMatrix.id, nodeIdToNeighbors))
    psMatrix.asyncPsfUpdate(func).get()
    nodeIdToNeighbors.clear()
    println(s"init ${pairs.length} long neighbors")
    table
  }

  def getNeighborTable(nodeIds: Array[Int]): Int2ObjectOpenHashMap[Array[Int]] = {
    val neighborsMap = table.sampleNeighbors(nodeIds, -1)
    neighborsMap
  }

  def getLongNeighborTable(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    val neighborsMap = table.sampleLongNeighbors(nodeIds, -1)
    neighborsMap
  }

  def checkpoint(): Unit = table.checkpoint()

  def testPS[ED: ClassTag](neighborsRDD: RDD[NeighborTablePartition[ED]],
                               num: Int = 10): NeighborTableModel = {
    val correct = neighborsRDD.map { part =>
      part.testPS(table, num)
    }.reduce(_ && _)
    assert(correct, "neighbor table is wrong")
    table
  }

  def calLinkPrediction[ED: ClassTag](neighborsRDD: RDD[NeighborTablePartition[ED]]): RDD[Edge[LinkPredictionMetric]] = {
    neighborsRDD.flatMap(_.calLinkPrediction(table))
  }

  def calTriangleUndirected[ED: ClassTag](neighborsRDD: RDD[NeighborTablePartition[ED]]): RDD[(Long, Long, Seq[(Long, Long)])] = {
    neighborsRDD.flatMap(_.calTriangleUndirected(table))
  }

}

object NeighborTableOps {

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }
}
