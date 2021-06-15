package com.tencent.angel.graph.embedding.metapath2vec

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.LongKeysUpdateParam
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.neighbor.simple.psf.init.InitNeighbors
import com.tencent.angel.graph.model.neighbor.simplewithtype.TypeNeighborElement
import com.tencent.angel.graph.model.neighbor.simplewithtype.psf.sample.{Sample, SampleParam}
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.util.VectorUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class MetaPath2VecPSModel(nbrModelContext: ModelContext,
                          tagModelContext: ModelContext = null) extends Serializable {
  var tagMatrix: PSMatrix = _
  var neighborMatrix: PSMatrix = _
  var tagVector: PSVector = _

  def init(): Unit = {
    val neighborMc = ModelContextUtils.createMatrixContext(nbrModelContext,
      RowType.T_ANY_LONGKEY_SPARSE, classOf[TypeNeighborElement])
    neighborMatrix = PSMatrix.matrix(neighborMc)

    if (tagModelContext != null) {
      val tagMc = ModelContextUtils.createMatrixContext(tagModelContext, RowType.T_INT_SPARSE_LONGKEY)
      tagMatrix = PSMatrix.matrix(tagMc)
      tagVector = new PSVectorImpl(tagMatrix.id, 0, tagModelContext.getMaxNodeId, tagMc.getRowType)
    }
  }

  def initMsgs(arr: Array[(Long, Int)], batchSize: Int): Iterator[Int] = {
    arr.sliding(batchSize, batchSize).map { iter =>
      val msgs = VFactory.sparseLongKeyIntVector(iter.length)
      for (idx <- iter.indices) {
        val (key, tag) = iter(idx)
        msgs.set(key, tag)
      }
      initMsgs(msgs)
      msgs.clear()
      iter.length
    }
  }

  def initMsgs(msgs: Vector): Unit =
    tagVector.update(msgs)

  def numMsgs(): Long = VectorUtils.nnz(tagVector)

  def readMsgs(nodes: Array[Long]): LongIntVector =
    tagVector.pull(nodes).asInstanceOf[LongIntVector]

  def initNeighbors(nodeIds: Array[Long], data: Array[IElement]): Unit = {
    val func = new InitNeighbors(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, data))
    neighborMatrix.asyncPsfUpdate(func).get()
    println(s"init ${nodeIds.length} type neighbors.")
  }

  def getTypes(iter: Array[Long], metaPath: Set[Int]) = {
    val pulled = readMsgs(iter)
    // make sure nodes' types is in metaPath
    iter.flatMap { x =>
      val t = pulled.get(x)
      if (metaPath.contains(t)) Iterator.single(x, t) else Iterator.empty
    }.toIterator
  }

  def sample(nodes: Array[Long], sampleType: Int) = {
    val pulled = neighborMatrix.psfGet(new Sample(new SampleParam(neighborMatrix.id, nodes, sampleType)))
      .asInstanceOf[GetLongsResult].getData
    val out = new Long2ObjectOpenHashMap[Iterator[Long]](pulled.size())
    val iter = pulled.long2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val next = iter.next()
      val data = next.getValue.toIterator
      out.put(next.getLongKey, data)
    }
    out
  }

  def checkpoint(): Unit = {
    neighborMatrix.checkpoint()
  }

}