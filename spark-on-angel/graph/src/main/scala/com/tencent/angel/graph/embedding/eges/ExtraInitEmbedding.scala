package com.tencent.angel.graph.embedding.eges

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.vector.element.{FloatArrayElement, IElement}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.{KeyValuePart, RouterUtils}
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyAnyValuePartOp
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import java.util
import scala.util.Random

class ExtraInitEmbedding(var param: ExtraInitEmbeddingParam) extends UpdateFunc(param) {

  def this() = this(null)

  /**
   * Partition update.
   *
   * @param partParam the partition parameter
   */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val adjustParam = partParam.asInstanceOf[PartExtraInitEmbeddingParam]
    val row = GraphMatrixUtils.getPSIntKeyRow(psContext, adjustParam)

    try {
      if (adjustParam.updates != null) {
        val data = adjustParam.updates.asInstanceOf[IIntKeyAnyValuePartOp]
        val nodes = data.getKeys
        val updates = data.getValues

        if (nodes != null) {
          val rand = new Random(System.currentTimeMillis())
          nodes.zip(updates).foreach(e => {
            val ele = row.get(e._1)
            if (ele == null) {
              val dim = e._2.asInstanceOf[FloatArrayElement].getData.length
              val embedding = new Array[Float](dim)
              for (i <- 0 until dim) {
                embedding(i) = (rand.nextFloat() - 0.5f) / dim
              }
              row.set(e._1, new EmbeddingNode(embedding))
            }
            row.get(e._1).asInstanceOf[EmbeddingNode].setFeats(e._2.asInstanceOf[FloatArrayElement].getData)
          })
        }
      }
    } finally {
      //row.endWrite()
    }
  }
}

class ExtraInitEmbeddingParam(matrixId: Int,
                              updates: Int2ObjectOpenHashMap[Array[Float]]) extends UpdateParam(matrixId) {
  /**
   * Split list.
   *
   * @return the list
   */
  override def split(): util.List[PartitionUpdateParam] = {
    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys

    val partParams = new util.ArrayList[PartitionUpdateParam](parts.length)
    val splits = splitIntFloatsMap(matrixMeta, updates)
    splits.zipWithIndex.foreach(e => {
      if (e._1 != null && e._1.size() > 0) {
        partParams.add(new PartExtraInitEmbeddingParam(matrixId, parts(e._2), splits(e._2)))
      }
    })

    partParams
  }

  def splitIntFloatsMap(matrixMeta: MatrixMeta, data: Int2ObjectOpenHashMap[Array[Float]]): Array[KeyValuePart] = {
    val nodeIds: Array[Int] = new Array[Int](data.size())
    val updates: Array[IElement] = new Array[IElement](data.size())

    if (data != null && data.size() > 0) {
      val iter = data.entrySet().iterator()
      var index = 0
      while (iter.hasNext) {
        val entry = iter.next()
        nodeIds(index) = entry.getKey
        updates(index) = new FloatArrayElement(entry.getValue)
        index += 1
      }

      RouterUtils.split(matrixMeta, 0, nodeIds, updates)
    } else {
      new Array[KeyValuePart](matrixMeta.getPartitionNum)
    }
  }
}

class PartExtraInitEmbeddingParam(matrixId: Int,
                                  part: PartitionKey,
                                  var updates: KeyValuePart)
  extends PartitionUpdateParam(matrixId, part) {

  def this() = this(-1, null, null)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    if (updates != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true)
      ByteBufSerdeUtils.serializeKeyValuePart(buf, updates)
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false)
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    if (ByteBufSerdeUtils.deserializeBoolean(buf)) {
      updates = ByteBufSerdeUtils.deserializeKeyValuePart(buf)
    }
  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += ByteBufSerdeUtils.BOOLEN_LENGTH
    if (updates != null) {
      len += ByteBufSerdeUtils.serializedKeyValuePartLen(updates)
    }
    len
  }
}

