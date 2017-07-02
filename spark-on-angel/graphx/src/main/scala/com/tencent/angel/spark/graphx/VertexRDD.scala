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

package com.tencent.angel.spark.graphx

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import com.tencent.angel.psagent.matrix.MatrixClient

abstract class VertexRDD[VD: ClassTag](
  sc: SparkContext,
  deps: Seq[Dependency[_]]) extends RDD[(VertexId, VD)](sc, deps) {

  private[graphx] def psName: String

  private[graphx] def isDense: Boolean

  private[graphx] def rowSize: Long

  private[graphx] def colSize: Long

  private[graphx] def partitionsRDD: RDD[VertexId]

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  def updateValues(data: RDD[(VertexId, VD)]): this.type

  def copy(withValues: Boolean): VertexRDD[VD]

  def destroy(blocking: Boolean): Unit

  override def compute(split: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    throw new NoSuchMethodException()
  }

  override def checkpoint(): Unit = {
    partitionsRDD.checkpoint()
  }

  override def localCheckpoint(): this.type = {
    partitionsRDD.localCheckpoint()
    this
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }
}
