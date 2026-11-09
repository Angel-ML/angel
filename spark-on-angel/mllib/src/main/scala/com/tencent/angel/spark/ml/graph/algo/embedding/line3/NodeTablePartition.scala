package com.tencent.angel.spark.ml.graph.algo.embedding.line3

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.server.data.request.UpdateOp
import com.tencent.angel.ps.storage.partition.UserDefinePartition
import com.tencent.angel.ps.storage.partition.storage.UserDefinePartitionStorage
import com.tencent.angel.ps.storage.vector.element.IElement
import io.netty.buffer.ByteBuf

import scala.util.Random

class NodeTablePartition(partKey: PartitionKey, rowType: RowType, estSparsity: Long,
                         storage: NodeTableStorage)
  extends UserDefinePartition(partKey, rowType, -1, storage) with INodeTableStorageOp {

  def this() = this(null, RowType.T_INT_DENSE, -1, null)

  /**
    * Init the storage
    */
  override def init(): Unit = {}

  /**
    * Reset the storage
    */
  override def reset(): Unit = getStorage.reset()

  override def negativeSample(srcNodes: Array[ReadOnlyByteArray],
                              dstNodes: Array[ReadOnlyByteArray],
                              count: Int): Array[Array[ReadOnlyByteArray]] = {
    getStorage.asInstanceOf[NodeTableStorage].negativeSample(srcNodes, dstNodes, count)
  }

  override def negativeSample(srcNodes: Array[IElement],
                              dstNodes: Array[IElement],
                              count: Int): Array[Array[ReadOnlyByteArray]] = {
    getStorage.asInstanceOf[NodeTableStorage].negativeSample(srcNodes, dstNodes, count)
  }
}

class NodeTableStorage(@transient var rowOffset: Int,
                       @transient var nodes: Array[ReadOnlyByteArray])
  extends UserDefinePartitionStorage(rowOffset) with INodeTableStorageOp {

  val r = new Random()

  /**
    * Init the storage
    */
  override def init(): Unit = {}

  /**
    * Reset the storage
    */
  override def reset(): Unit = {}

  /**
    * Update the partition use pipeline mode
    *
    * @param buf input buf
    * @param op  update method
    */
  override def update(buf: ByteBuf, op: UpdateOp): Unit = {}

  /**
    * Get total element number in partition
    *
    * @return total element in partition
    */
  override def getElemNum: Long = nodes.length

  override def negativeSample(srcNodes: Array[ReadOnlyByteArray],
                              dstNodes: Array[ReadOnlyByteArray],
                              count: Int): Array[Array[ReadOnlyByteArray]] = {
    srcNodes.zip(dstNodes).map(e => {
      val samples = new Array[ReadOnlyByteArray](count)
      var sampleIndex = 0
      while (sampleIndex < count) {
        val sample = nodes(r.nextInt(nodes.length))
        if (!sample.equals(e._1) && !sample.equals(e._2)) {
          samples(sampleIndex) = sample
          sampleIndex += 1
        }
      }
      samples
    })
  }

  override def negativeSample(srcNodes: Array[IElement],
                              dstNodes: Array[IElement],
                              count: Int): Array[Array[ReadOnlyByteArray]] = {
    srcNodes.zip(dstNodes).map(e => {
      val samples = new Array[ReadOnlyByteArray](count)
      var sampleIndex = 0
      while (sampleIndex < count) {
        val sample = nodes(r.nextInt(nodes.length))
        if (!sample.equals(e._1) && !sample.equals(e._2)) {
          samples(sampleIndex) = sample
          sampleIndex += 1
        }
      }
      samples
    })
  }
}

trait INodeTableStorageOp {
  def negativeSample(srcNodes: Array[ReadOnlyByteArray],
                     dstNodes: Array[ReadOnlyByteArray],
                     count: Int): Array[Array[ReadOnlyByteArray]]

  def negativeSample(srcNodes: Array[IElement],
                     dstNodes: Array[IElement],
                     count: Int): Array[Array[ReadOnlyByteArray]]
}
