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

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * The Graph abstractly represents a graph with arbitrary objects
 * associated with vertices and edges.  The graph provides basic
 * operations to access and manipulate the data associated with
 * vertices and edges as well as the underlying structure.  Like Spark
 * RDDs, the graph is a functional data-structure in which mutating
 * operations return new graphs.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
abstract class Graph[VD: ClassTag, ED: ClassTag] protected() extends Serializable {

  def vertices: VertexRDD[VD]

  def edges: RDD[Edge[ED]]

  def triplets(tripletFields: TripletFields = TripletFields.All): RDD[EdgeTriplet[VD, ED]]

  def updateVertices(data: RDD[(VertexId, VD)]): Unit

  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]

  def unpersist(blocking: Boolean = true): Graph[VD, ED]

  def destroy(blocking: Boolean): Unit

  def checkpoint(): Unit

  def isCheckpointed: Boolean

  def getCheckpointFiles: Seq[String]

  def subgraph(
    epred: EdgeTriplet[VD, ED] => Boolean = x => true,
    vpred: (VertexId, VD) => Boolean = (v, d) => true): Graph[VD, ED]

  def map[ED2: ClassTag](
    fn: (EdgeTriplet[VD, ED], VertexCollector[VD, ED]) => ED2,
    tripletFields: TripletFields): Graph[VD, ED2] = {
    val cleanF = clean(fn)
    map((pid, iter, out) => iter.map { e => Edge(e.srcId, e.dstId, cleanF(e, out)) }, tripletFields)
  }

  def map[ED2: ClassTag](
    fn: (PartitionID, Iterator[EdgeTriplet[VD, ED]], VertexCollector[VD, ED]) => Iterator[Edge[ED2]],
    tripletFields: TripletFields): Graph[VD, ED2]

  def foreach(
    fn: (EdgeTriplet[VD, ED], VertexCollector[VD, ED]) => Unit,
    tripletFields: TripletFields): Unit = {
    val cleanF = clean(fn)
    foreach((pid, iter, out) => iter.foreach { e => cleanF(e, out) }, tripletFields)
  }

  def foreach(
    fn: (PartitionID, Iterator[EdgeTriplet[VD, ED]], VertexCollector[VD, ED]) => Unit,
    tripletFields: TripletFields): Unit

  private[graphx] def aggregateMessages[VD2: ClassTag](
    fn: (EdgeTriplet[VD, ED], VertexCollector[VD2, ED]) => Unit,
    tripletFields: TripletFields = TripletFields.All): VertexRDD[VD2]

  def mapReduceTriplets[VD2: ClassTag](
    mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, VD2)],
    reduceFunc: (VD2, VD2) => VD2,
    tripletFields: TripletFields = TripletFields.All): RDD[(VertexId, VD2)]

  def mapVertices[VD2: ClassTag](fn: (VertexId, VD) => VD2): Graph[VD2, ED]

  def mapEdges[ED2: ClassTag](fn: Edge[ED] => ED2): Graph[VD, ED2] = {
    val cleanFn = clean(fn)
    mapEdges((pid, iter) => iter.map(e => Edge[ED2](e.srcId, e.dstId, cleanFn(e))))
  }

  def mapEdges[ED2: ClassTag](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[Edge[ED2]]): Graph[VD, ED2]

  def mapTriplets[ED2: ClassTag](fn: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] = {
    val cleanFn = clean(fn)
    mapTriplets((pid, iter) => iter.map(e => Edge[ED2](e.srcId, e.dstId, cleanFn(e)).
      toEdgeTriplet(e.srcAttr, e.dstAttr)), TripletFields.All)
  }

  def mapTriplets[ED2: ClassTag](
    fn: EdgeTriplet[VD, ED] => ED2,
    tripletFields: TripletFields): Graph[VD, ED2] = {
    val cleanFn = clean(fn)
    mapTriplets((pid, iter) => iter.map(e => Edge[ED2](e.srcId, e.dstId, cleanFn(e)).
      toEdgeTriplet(e.srcAttr, e.dstAttr)), tripletFields)
  }

  def mapTriplets[ED2: ClassTag](
    fn: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[Edge[ED2]],
    tripletFields: TripletFields): Graph[VD, ED2]

  private[graphx] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    util.ClosureCleaner.clean(f, checkSerializable)
    f
  }
}

