package com.tencent.angel.spark.ml.graph.algo.rank.intimacy

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.model.neighbor.simple.psf.init.InitNeighbors
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.GetNeighborsAttrs.{GetNeighborsAttrs, GetNeighborsAttrsResult}
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.InitNeighborsAttrs.NeighborsAttrsElement
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class IntimacyPSModel(val psMatrix: PSMatrix) extends Serializable {

  def initLongNeighborAttrs(neighborTable: RDD[(Long, Array[(Long, Float)])], batchSize: Int):
  PSMatrix = {
    neighborTable.mapPartitions { iter => {
        iter.sliding(batchSize, batchSize).map(pairs => initLongNeighborAttrs(pairs))
      }
    }.count()
    psMatrix
  }

  def initLongNeighborAttrs(pairs: Seq[(Long, Array[(Long, Float)])]): Int = {
    val nodeIds = new Array[Long](pairs.size)
    val nodeIdToNeighbors = new Array[IElement](pairs.size)
    pairs.zipWithIndex.foreach { case (iter, index) =>
      nodeIds(index) = iter._1
      val (neis, attrs) = iter._2.unzip
      val element = new NeighborsAttrsElement(neis, attrs)
      nodeIdToNeighbors(index) = element
    }
    psMatrix.psfUpdate(new InitNeighbors(new LongKeysUpdateParam(psMatrix.id, nodeIds
      , nodeIdToNeighbors))).get()
    println(s"init ${pairs.length} long neighbors")

    pairs.length
  }

  def getLongNeighborAttrs(nodeIds: Array[Long]): Long2ObjectOpenHashMap[NeighborsAttrsElement] = {


    psMatrix.psfGet(new GetNeighborsAttrs(new LongKeysGetParam(psMatrix.id, nodeIds)))
      .asInstanceOf[GetNeighborsAttrsResult].getNodeIdeToNeighbors
  }
}

object IntimacyPSModel {

  def fromMinMax(minId: Long, maxId: Long, psNumPartition: Int, data: RDD[(Long, (Long, Float))],
                 useBalancePartition: Boolean = false, storageLevel: StorageLevel, percent: Float):
  IntimacyPSModel = {

    val modelContext = new ModelContext(psNumPartition, minId, maxId, -1,
      "neighborTable", SparkContext.getOrCreate().hadoopConfiguration)
    val mc = ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE,
      classOf[NeighborsAttrsElement])

    var index: RDD[Long] = null
    if (useBalancePartition && (!modelContext.isUseHashPartition)) {
      index = data.flatMap(f => Iterator(f._1, f._2._1))
      LoadBalancePartitioner.partition(index, modelContext.getMaxNodeId,
        modelContext.getPartitionNum, mc, percent)
    }

    val psMatrix = PSMatrix.matrix(mc)

    if (useBalancePartition && (!modelContext.isUseHashPartition))
      index.unpersist()

    new IntimacyPSModel(psMatrix)
  }
}
