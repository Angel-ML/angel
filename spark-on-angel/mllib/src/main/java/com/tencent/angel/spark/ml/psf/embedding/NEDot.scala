/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.spark.ml.psf.embedding

import scala.collection.JavaConversions._

import io.netty.buffer.ByteBuf

import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.matrix.ServerPartition
import com.tencent.angel.spark.ml.psf.embedding.NEDot.{DotPartitionResult, NEDotResult}

abstract class NEDot(param: GetParam) extends GetFunc(param) {
  def this() = this(null)

  override def merge(partResults: java.util.List[PartitionGetResult]): GetResult = {
    if (partResults.size() == 0) null else {
      val arr = partResults.map(_.asInstanceOf[DotPartitionResult].getResult)
      val result: Array[Float] = arr.head
      arr.tail.foreach(other => other.indices.foreach(i => result(i) += other(i)))
      new NEDotResult(result)
    }
  }

  override def partitionGet(pParam: PartitionGetParam): PartitionGetResult = {
    val part = psContext.getMatrixStorageManager.getPart(pParam.getMatrixId, pParam.getPartKey.getPartitionId)
    if (part == null) null else new DotPartitionResult(doProcess(part, pParam))
  }

  def doProcess(part: ServerPartition, pParam: PartitionGetParam): Array[Float]
}

object NEDot {

  class NEDotResult(val result: Array[Float]) extends GetResult

  class DotPartitionResult(private var result: Array[Float]) extends PartitionGetResult {
    def this() = this(null)

    def getResult: Array[Float] = result

    override def serialize(buf: ByteBuf) {
      buf.writeInt(result.length)
      result.foreach(buf.writeFloat)
    }

    override def deserialize(buf: ByteBuf) {
      val size = buf.readInt()
      result = new Array[Float](size)
      for (i <- result.indices) result(i) = buf.readFloat()
    }

    override def bufferLen(): Int = 4 + result.length * 4
  }

}