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

package com.tencent.angel.spark.ml.embedding.line

import java.util.UUID

import com.tencent.angel.ps.storage.matrix.{PSMatrixInit, ServerMatrix}
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow
import io.netty.buffer.ByteBuf

import scala.util.Random
import scala.collection.JavaConversions._

class LINEInitFunc(var order:Int, var dim:Int) extends PSMatrixInit {

  def this() = this(-1, -1)

  override def init(psMatrix: ServerMatrix): Unit = {
    val parts = psMatrix.getPartitions.values()
    val seed = UUID.randomUUID().hashCode();
    val rand = new Random(seed)

    for(part <- parts) {
      val row: ServerIntAnyRow = part.asInstanceOf[RowBasedPartition].getRow(0).asInstanceOf[ServerIntAnyRow]
      println(s"random seed in init=$seed")

      (row.getStartCol until row.getEndCol).foreach(colId => {
        val embedding = new Array[Float](dim)
        for (i <- 0 until dim) {
          embedding(i) = (rand.nextFloat() - 0.5f) / dim
        }
        if(order == 1) {
          row.set(colId.toInt, new LINENode(embedding, null))
        } else {
          row.set(colId.toInt, new LINENode(embedding, new Array[Float](dim)))
        }
      })
    }
  }

  override def serialize(output: ByteBuf): Unit = {
    output.writeInt(order)
    output.writeInt(dim)
  }

  override def deserialize(input: ByteBuf): Unit = {
    order = input.readInt()
    dim = input.readInt()
  }

  override def bufferLen(): Int = 8
}
