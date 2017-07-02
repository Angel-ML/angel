/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.spark.graphx.impl

import java.util.UUID

import scala.reflect.ClassTag

import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.PSContext
import com.tencent.angel.spark.graphx.util.CompletionIterator
import com.tencent.angel.spark.graphx.ps.{PSUtils, Operation => PSop}
import com.tencent.angel.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark._
import org.apache.spark.rdd._

class VertexRDDImpl[VD: ClassTag] private[graphx](
  @transient override val partitionsRDD: RDD[VertexId],
  override val psName: String,
  override val isDense: Boolean,
  override val rowSize: Long,
  override val colSize: Long)
  extends VertexRDD[VD](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {
  @transient protected lazy val vdTag: ClassTag[VD] = implicitly[ClassTag[VD]]
  private[graphx] var batchSize: Int = 1000
  val poolId = -1

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * Provides the `RDD[(VertexId, VD)]` equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    firstParent[VertexId].iterator(part, context).grouped(batchSize).map { ids =>
      val values = PSop.get[VD](psName, ids.map(_.toInt).toArray)
      ids.zip(values).toIterator
    }.flatten
  }

  override def updateValues(data: RDD[(VertexId, VD)]): this.type = {
    data.foreachPartition { iter =>
      iter.grouped(batchSize).foreach { p =>
        val (indices, values) = p.unzip
        PSop.update(psName, indices.map(_.toInt).toArray, values.toArray)
      }
    }
    this
  }

  override def copy(withValues: Boolean): VertexRDD[VD] = {
    GraphImpl.vertices2vertexRDD(this, this.partitionsRDD, this.rowSize, this.colSize)
  }

  override def destroy(blocking: Boolean): Unit = {
    PSop.remove[VD](psName)
  }
}
