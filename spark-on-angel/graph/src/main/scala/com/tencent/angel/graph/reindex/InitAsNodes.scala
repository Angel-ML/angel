package com.tencent.angel.graph.reindex

import java.util
import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.vector.element.{IElement, LongElement}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.operator.IAnyKeyAnyValuePartOp
import com.tencent.angel.psagent.matrix.transport.router.{KeyValuePart, RouterUtils}
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap

class InitAsNodes(var param: InitAsNodesParam) extends UpdateFunc(param) {

  def this() = this(null)

  /**
    * Partition update.
    *
    * @param partParam the partition parameter
    */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val initParam = partParam.asInstanceOf[PartInitAsNodesParam]
    val row = GraphMatrixUtils.getPSAnyKeyRow(psContext, initParam)
    row.startWrite()
    try {
      if (initParam.updates != null) {
        val inputData = initParam.updates.asInstanceOf[IAnyKeyAnyValuePartOp]
        val inputNodes = inputData.getKeys
        val inputUpdates = inputData.getValues

        if(inputNodes != null) {
          inputNodes.zip(inputUpdates).foreach(e => {
            row.set(e._1, new ReIndexValue(e._2.asInstanceOf[LongElement].getData))
          })
        }
      }
    } finally {
      row.endWrite()
    }
  }
}

class InitAsNodesParam(matrixId: Int,
                       updates: Object2LongOpenHashMap[ReadOnlyByteArray]) extends UpdateParam(matrixId) {
  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionUpdateParam] = {
    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys

    val partParams = new util.ArrayList[PartitionUpdateParam](parts.length)
    val splits = splitAnyLongsMap(matrixMeta, updates)
    splits.zipWithIndex.foreach(e => {
      if (e._1 != null && e._1.size() > 0) {
        partParams.add(
          new PartInitAsNodesParam(matrixId, parts(e._2), splits(e._2)))
      }
    })
    partParams
  }

  def splitAnyLongsMap(matrixMeta: MatrixMeta, data: Object2LongOpenHashMap[ReadOnlyByteArray]): Array[KeyValuePart] = {
    val nodeIds: Array[IElement] = new Array[IElement](data.size())
    val values: Array[IElement] = new Array[IElement](data.size())

    if (data != null && data.size() > 0) {
      val iter = data.entrySet().iterator()
      var index = 0
      while (iter.hasNext) {
        val entry = iter.next()
        nodeIds(index) = entry.getKey
        values(index) = new LongElement(entry.getValue)
        index += 1
      }
      RouterUtils.split(matrixMeta, 0, nodeIds, values)
    } else {
      new Array[KeyValuePart](matrixMeta.getPartitionNum)
    }
  }
}

class PartInitAsNodesParam(matrixId: Int,
                           part: PartitionKey,
                           var updates: KeyValuePart) extends PartitionUpdateParam(matrixId, part) {

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
