/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving

import com.tencent.angel.common.Serialize
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.transport.Encodable
import io.netty.buffer.ByteBuf

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

/**
  * the chunked shading data, keep the dimension with each sharding matrix of model
  *
  * @param rowType the predict data type
  * @tparam V
  */
class ChunkedShardingData[V <: TVector : TypeTag](val rowType: RowType) extends ShardingData with Encodable {

  {
    require(rowType == getRowType(typeOf[V]), s"$rowType is not match with " + typeOf[V])
  }
  val is64Key = rowType == RowType.T_DOUBLE_SPARSE_LONGKEY

  private[this] def getRowType(rowType: Type): RowType = {
    rowType match {
      case t if t == typeOf[DenseIntVector] => RowType.T_INT_DENSE
      case t if t == typeOf[DenseDoubleVector] => RowType.T_DOUBLE_DENSE
      case t if t == typeOf[DenseFloatVector] => RowType.T_FLOAT_DENSE
      case t if t == typeOf[SparseIntVector] => RowType.T_INT_SPARSE
      case t if t == typeOf[SparseDoubleVector] => RowType.T_DOUBLE_SPARSE
      case t if t == typeOf[SparseFloatVector] => RowType.T_FLOAT_SPARSE
      case t if t == typeOf[SparseLongKeyDoubleVector] => RowType.T_DOUBLE_SPARSE_LONGKEY
      case t => throw new AngelException("unsupported type " + t)
    }
  }


  val indexData = new ArrayBuffer[(Long, Long, SingleShardingData[V])]()

  def insert(offset: Long, data: V): Unit = {
    val dimension = if (is64Key) data.asInstanceOf[TLongDoubleVector].getLongDim else data.getDimension
    //intersect
    indexData.find { case (start, len, _) => (start < offset && start + len > offset) || (offset < start && offset + dimension > start) }
      .foreach { case (start, len, _) => throw new AngelException(s"(offset:$offset,dimension:$dimension) is intersect with ($start,$len)") }
    //contain
    indexData.zipWithIndex.find { case ((start: Long, len: Long, _), _: Int) => offset <= start && len <= dimension }
      .map { case ((_, _, _), idx: Int) => idx }.foreach(idx => {
      indexData.remove(idx)
    })

    val tuple = (offset, dimension, new SingleShardingData[V](vector = data))
    indexData += tuple
  }


  override def getData(offset: Long, dimension: Long): V = {
    indexData.find { case (start: Long, len: Long, _) => offset >= start && dimension <= len }
      .map { case (_, _, data) => data.getData(offset, dimension) }.orElse{
        val gaps = indexData.filter { case (start: Long, len: Long, _) =>
          (start < offset && start + len > offset) ||
            (start < offset + dimension && start + len > (offset + dimension) ||
              start > offset && start + len < offset + dimension)
        }.sortBy(_._1).map { case (start, len, _) => s"($start,$len)"
        }.reduce((gap1: String, gap2: String) => gap1 + "," + gap2)
        throw new AngelException(if (gaps.isEmpty) s"(offset:$offset,dimension:$dimension) not found" else s"(offset:$offset,dimension:$dimension) has gap: $gaps")
      }.get
  }

  /**
    * Serialize object to the Netty ByteBuf.
    *
    * @param buf the Netty ByteBuf
    */
  override def encode(buf: ByteBuf) = {
    buf.writeInt(rowType.getNumber)
    buf.writeInt(indexData.size)
    indexData.foreach {
      case (start, len, data) => {
        buf.writeLong(start)
        buf.writeLong(len)
        require(data.get.isInstanceOf[Serialize])
        data.get.asInstanceOf[Serialize].serialize(buf)
      }
    }
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def encodedLength(): Int = {
    4 + 4 + indexData.map { case (_, _, data) => 8 + 8 + data.get.asInstanceOf[Serialize].bufferLen() }.sum
  }

}

object ChunkedShardingData {

  def apply(rowType: RowType): ChunkedShardingData[_] = {
    rowType match {
      case RowType.T_INT_DENSE => new ChunkedShardingData[DenseIntVector](rowType)
      case RowType.T_DOUBLE_DENSE => new ChunkedShardingData[DenseDoubleVector](rowType)
      case RowType.T_FLOAT_DENSE => new ChunkedShardingData[DenseFloatVector](rowType)
      case RowType.T_INT_SPARSE => new ChunkedShardingData[SparseIntVector](rowType)
      case RowType.T_DOUBLE_SPARSE => new ChunkedShardingData[SparseDoubleVector](rowType)
      case RowType.T_FLOAT_SPARSE => new ChunkedShardingData[SparseFloatVector](rowType)
      case RowType.T_DOUBLE_SPARSE_LONGKEY => new ChunkedShardingData[SparseLongKeyDoubleVector](rowType)
      case _ => throw new AngelException("unsupported type " + rowType)
    }
  }


  def decode(rowType: RowType, buf: ByteBuf): ChunkedShardingData[TVector] = {
    val chunked = ChunkedShardingData(rowType).asInstanceOf[ChunkedShardingData[TVector]]
    val len = buf.readInt()
    (0 until len).foreach(_ => {
      val offset = buf.readLong()
      val dim = buf.readLong()
      val data: TVector = Util.initVector(rowType, dim)
      data.asInstanceOf[Serialize].deserialize(buf)
      chunked.insert(offset, data)
    })
    chunked
  }
}

class ChunkedShardingDataList[V <: TVector](val rowType: RowType, val data: Array[ChunkedShardingData[V]]) extends Encodable {
  /** Number of bytes of the encoded form of this object. */
  override def encodedLength: Int = 1 + data.map(_.encodedLength()).sum

  /**
    * Serializes this object by writing into the given ByteBuf.
    * This method must write exactly encodedLength() bytes.
    */
  override def encode(buf: ByteBuf): Unit = {
    buf.writeInt(rowType.getNumber)
    buf.writeInt(data.length)
    data.foreach(_.encode(buf))
  }
}

object ChunkedShardingDataList {
  def decode[V <: TVector](buf: ByteBuf): ChunkedShardingDataList[V] = {
    val rowType = RowType.valueOf(buf.readInt())
    val len = buf.readInt()
    val data = (0 until len).map(_ => {
      val chunk = ChunkedShardingData.decode(rowType, buf).asInstanceOf[ChunkedShardingData[V]]
      chunk
    }).toArray
    new ChunkedShardingDataList[V](rowType, data)
  }
}



