package com.tencent.angel.spark.ml.tree.kdtree.psmodel

import com.tencent.angel.graph.client.getnodefeats2.{GetNodeFeats, GetNodeFeatsParam, GetNodeFeatsResult}
import com.tencent.angel.graph.data.Node
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl
import org.apache.spark.rdd.RDD
import com.tencent.angel.graph.client.initnodefeats4.{UpdateNodeFeats, InitNodeFeats => InitNodeFeats4, InitNodeFeatsParam => InitNodeFeatsParam4}
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.model.output.format.RowIdColIdValueTextRowFormat
import com.tencent.angel.spark.context.AngelPSContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class KDTreePSModel(psMatrix: PSMatrix)  extends Serializable {

  def initNodeFeatures(keys: Array[Long], features: Array[IntFloatVector]): Unit = {
    initNodeFeatures(keys, features, 0, keys.length)
  }

  def initNodeFeatures(keys: Array[Long], features: Array[IntFloatVector],
                       start: Int, end: Int): Unit = {
    val param = new InitNodeFeatsParam4(psMatrix.id, keys, features, start, end)
    val func = new InitNodeFeats4(param)
    psMatrix.psfUpdate(func).get()
  }

  def getFeatures(keys: Array[Long]): Long2ObjectOpenHashMap[IntFloatVector] = {
    val func = new GetNodeFeats(new GetNodeFeatsParam(psMatrix.id, keys.clone()))
    psMatrix.psfGet(func).asInstanceOf[GetNodeFeatsResult].getResult
  }

  def updateFeatures(keys: Array[Long], features: Array[IntFloatVector]): Unit = {
    val param = new InitNodeFeatsParam4(psMatrix.id, keys, features, 0, keys.length)
    val func = new UpdateNodeFeats(param)
    psMatrix.psfUpdate(func).get()
  }

  def save(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext = new ModelSaveContext(path)
    val matrixContext = new MatrixSaveContext("instance", format)
    matrixContext.addIndices(Array(0, 1))
    modelContext.addMatrix(matrixContext)
    AngelPSContext.save(modelContext)
  }

}

object KDTreePSModel {
  def apply(minId: Long, maxId: Long,
            index: RDD[Long], psNumPartition: Int): KDTreePSModel = {

    val ctx = new MatrixContext("instance", 1, minId, maxId)
    ctx.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    ctx.setValueType(classOf[Node])
    LoadBalancePartitioner.partition(index, maxId, psNumPartition, ctx, 0.5F)

    val ctxId = PSMatrixUtils.createPSMatrix(ctx)
    val instancePSMatrix = new PSMatrixImpl(ctxId, 1, maxId, ctx.getRowType)

    new KDTreePSModel(instancePSMatrix)
  }
}
