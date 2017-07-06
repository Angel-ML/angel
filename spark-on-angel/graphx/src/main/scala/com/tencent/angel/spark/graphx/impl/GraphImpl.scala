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

import scala.collection.mutable
import scala.reflect.ClassTag

import com.tencent.angel.spark.graphx._
import com.tencent.angel.spark.graphx.ps.{Operation => PSop, PSUtils => GPSUtils}
import org.apache.spark.TaskContext
import org.apache.spark.mllib.linalg.{DenseVector => SDV, SparseVector => SSV, Vector => SV}
import breeze.linalg.{DenseVector => BDV, Matrix => BM, SparseVector => BSV, Vector => BV}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


class GraphImpl[VD: ClassTag, ED: ClassTag](
  @transient override val vertices: VertexRDD[VD],
  @transient override val edges: RDD[Edge[ED]]) extends Graph[VD, ED] {

  private var batchSize: Int = 1000

  override def updateVertices(data: RDD[(VertexId, VD)]): Unit = {
    vertices.asInstanceOf[VertexRDDImpl[VD]].updateValues(data)
  }

  override def map[ED2: ClassTag](
    fn: (PartitionID, Iterator[EdgeTriplet[VD, ED]], VertexCollector[VD, ED]) => Iterator[Edge[ED2]],
    tripletFields: TripletFields): Graph[VD, ED2] = {
    val vName = vertices.psName
    val cleanFn = clean(fn)
    val newEdges = triplets(tripletFields).mapPartitionsWithIndex { case (pid, iter) =>
      val edgeContext = new VertexCollectorImpl[VD, ED](vName)
      cleanFn(pid, iter, edgeContext)
    }
    new GraphImpl[VD, ED2](vertices, newEdges)
  }

  override def foreach(
    fn: (PartitionID, Iterator[EdgeTriplet[VD, ED]], VertexCollector[VD, ED]) => Unit,
    tripletFields: TripletFields): Unit = {
    val vName = vertices.psName
    val cleanFn = clean(fn)
    val func: (TaskContext, Iterator[EdgeTriplet[VD, ED]]) => Unit = (tc, iter) => {
      val out = new VertexCollectorImpl[VD, ED](vName)
      val pid = tc.partitionId()
      cleanFn(pid, iter, out)
    }
    val cleanFunc = clean(func)
    val sc = edges.sparkContext
    sc.runJob[EdgeTriplet[VD, ED], Unit](triplets(tripletFields), cleanFunc)

  }

  override private[graphx] def aggregateMessages[VD2: ClassTag](
    fn: (EdgeTriplet[VD, ED], VertexCollector[VD2, ED]) => Unit,
    tripletFields: TripletFields = TripletFields.All): VertexRDD[VD2] = {
    val vi = vertices.asInstanceOf[VertexRDDImpl[VD]]
    val rowSize = vi.rowSize
    val colSize = 2
    val isDense = vi.isDense
    val cleanFn = clean(fn)
    val newName = PSop.create[VD2](isDense, rowSize.toInt, colSize.toInt)
    triplets(tripletFields).foreachPartition { iter =>
      val edgeContext = new VertexCollectorImpl[VD2, ED](newName)
      iter.foreach(e => cleanFn(e, edgeContext))
    }
    new VertexRDDImpl[VD2](vi.partitionsRDD, newName, isDense, rowSize, colSize)
  }

  override def mapReduceTriplets[VD2: ClassTag](
    mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, VD2)],
    reduceFunc: (VD2, VD2) => VD2,
    tripletFields: TripletFields = TripletFields.All): RDD[(VertexId, VD2)] = {
    val cleanMapFunc = clean(mapFunc)
    val cleanReduceFunc = clean(reduceFunc)
    val data = triplets(tripletFields).flatMap(cleanMapFunc)
    data.partitioner.map(partitioner => data.reduceByKey(partitioner, cleanReduceFunc)).
      getOrElse(data.reduceByKey(cleanReduceFunc, data.partitions.length))
  }

  override def mapVertices[VD2: ClassTag](fn: (VertexId, VD) => VD2): Graph[VD2, ED] = {
    val vi = vertices.asInstanceOf[VertexRDDImpl[VD]]
    val partitionsRDD = vi.partitionsRDD
    val rowSize = vi.rowSize
    val cleanFn = clean(fn)
    val data = vertices.map { case (vid, value) =>
      (vid, cleanFn(vid, value))
    }
    val newVertices = GraphImpl.vertices2vertexRDD[VD2](data, partitionsRDD, rowSize)
    new GraphImpl[VD2, ED](newVertices, edges)
  }

  override def mapEdges[ED2: ClassTag](
    fn: (PartitionID, Iterator[Edge[ED]]) =>
      Iterator[Edge[ED2]]): Graph[VD, ED2] = {
    val cleanFn = clean(fn)
    val newEdges = edges.mapPartitionsWithIndex { case (pid, iter) =>
      cleanFn(pid, iter)
    }
    new GraphImpl[VD, ED2](vertices, newEdges)
  }

  override def mapTriplets[ED2: ClassTag](
    fn: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[Edge[ED2]],
    tripletFields: TripletFields): Graph[VD, ED2] = {
    val cleanFn = clean(fn)
    val newEdges = triplets(tripletFields).mapPartitionsWithIndex { case (pid, iter) =>
      cleanFn(pid, iter)
    }
    new GraphImpl[VD, ED2](vertices, newEdges)
  }

  override def persist(newLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): this.type = {
    edges.persist(newLevel)
    vertices.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    edges.unpersist(blocking)
    vertices.unpersist(blocking)
    this
  }

  /**
   * TODO:  临时这样处理.
   *
   * @param blocking
   */
  override def destroy(blocking: Boolean): Unit = {
    edges.unpersist(blocking)
    vertices.unpersist(blocking)
    vertices.destroy(blocking)
  }

  /**
   * TODO:  临时这样处理.
   */
  override def checkpoint(): Unit = {
    edges.checkpoint()
    vertices.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    edges.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    edges.getCheckpointFile.toSeq
  }

  override def subgraph(
    epred: EdgeTriplet[VD, ED] => Boolean = x => true,
    vpred: (VertexId, VD) => Boolean = (v, d) => true): Graph[VD, ED] = {
    val newEdges = triplets(TripletFields.All).filter(epred).map(e => e.asInstanceOf[Edge[ED]])
    val cleanFn = clean(vpred)
    val v = vertices.filter(v => cleanFn(v._1, v._2))
    val vi = vertices.asInstanceOf[VertexRDDImpl[VD]]
    val isDense = vi.isDense
    val rowSize = vi.rowSize
    val colSize = vi.colSize
    val vName = PSop.create[VD](isDense, rowSize.toInt, colSize.toInt)
    val newVertices = new VertexRDDImpl[VD](v.map(_._1), vName, isDense, rowSize, colSize)
    newVertices.updateValues(v)
    new GraphImpl[VD, ED](newVertices, newEdges)
  }

  override def triplets(tripletFields: TripletFields = TripletFields.All): RDD[EdgeTriplet[VD, ED]] = {
    val vName = vertices.psName
    edges.mapPartitionsWithIndex { (pid, iter) =>
      if (tripletFields.useSrc || tripletFields.useDst) {
        val newIter = iter.grouped(batchSize).map { batchEdges =>
          val vidSet = mutable.HashSet[Long]()
          batchEdges.foreach { e =>
            if (tripletFields.useSrc) vidSet.add(e.srcId)
            if (tripletFields.useDst) vidSet.add(e.dstId)
          }
          val indices = vidSet.toArray
          val v2i = new mutable.OpenHashMap[Long, Int]()
          indices.zipWithIndex.foreach { case (vid, offset) =>
            v2i(vid) = offset
          }
          val vd = PSop.get[VD](vName, indices.map(_.toInt))
          batchEdges.map { edge =>
            var srcAttr: VD = null.asInstanceOf[VD]
            var dstAttr: VD = null.asInstanceOf[VD]
            if (tripletFields.useSrc) srcAttr = vd(v2i(edge.srcId))
            if (tripletFields.useDst) dstAttr = vd(v2i(edge.dstId))
            edge.toEdgeTriplet(srcAttr, dstAttr)
          }
        }.flatten
        newIter
      } else {
        iter.map { edge =>
          val srcAttr: VD = null.asInstanceOf[VD]
          val dstAttr: VD = null.asInstanceOf[VD]
          edge.toEdgeTriplet(srcAttr, dstAttr)
        }
      }
    }
  }
}

object GraphImpl {

  def apply[VD: ClassTag, ED: ClassTag](
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    fromEdgeRDD(edges, defaultVertexAttr)
  }

  def apply[VD: ClassTag, ED: ClassTag](
    vertices: RDD[(VertexId, VD)],
    edges: RDD[Edge[ED]]): GraphImpl[VD, ED] = {
    fromExistingRDDs(vertices, edges)
  }

  def apply[VD: ClassTag, ED: ClassTag](
    vertices: VertexRDD[VD],
    edges: RDD[Edge[ED]]): GraphImpl[VD, ED] = {
    fromExistingRDDs(vertices, edges)
  }

  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
    vertices: VertexRDD[VD],
    edges: RDD[Edge[ED]]): GraphImpl[VD, ED] = {
    new GraphImpl[VD, ED](vertices, edges)
  }

  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
    vertices: RDD[(VertexId, VD)],
    edges: RDD[Edge[ED]]): GraphImpl[VD, ED] = {
    val ids = (vertices.map(_._1) ++ edges.flatMap(e => Array(e.srcId, e.dstId))).
      distinct().persist(vertices.getStorageLevel)
    val vertexRDD = vertices2vertexRDD(vertices, ids)
    fromExistingRDDs(vertexRDD, edges)
  }

  def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    val vertices = edges
      .flatMap(e => Array(e.srcId, e.dstId))
      .distinct()
      .map(t => (t, defaultVertexAttr))
    fromExistingRDDs(vertices, edges)
  }

  private[graphx] def vertices2vertexRDD[VD: ClassTag](
    vertices: RDD[(VertexId, VD)],
    partitionsRDD: RDD[VertexId] = null,
    rowSize: Long = -1, colSize: Long = -1): VertexRDD[VD] = {
    val vdClass = implicitly[ClassTag[VD]].runtimeClass

    val ids = if (partitionsRDD == null) vertices.map(_._1) else partitionsRDD
    var isDense: Boolean = false
    val rowNum: Long = if (rowSize > 0) rowSize else ids.max() + 1
    var colNum: Long = if (colSize > 0) colSize else Int.MaxValue

    if (classOf[SV].isAssignableFrom(vdClass)) {
      isDense = !(vertices.filter(_._2.isInstanceOf[SSV]).count() < 1)
      isDense = isDense && !(vertices.map(_._2.asInstanceOf[SV].size).distinct().count() == 1)
      if (colSize < 0) colNum = vertices.map(_._2.asInstanceOf[SV].size).max()

    } else if (classOf[BV[_]].isAssignableFrom(vdClass)) {
      isDense = !(vertices.filter(_._2.isInstanceOf[BSV[_]]).count() < 1)
      isDense = isDense && !(vertices.map(_._2.asInstanceOf[BV[_]].length).distinct().count() == 1)
      if (colSize < 0) colNum = vertices.map(_._2.asInstanceOf[BV[_]].size).max()
    }

    val psName = PSop.create[VD](isDense, rowNum.toInt, colNum.toInt)
    val vertexRDD = new VertexRDDImpl[VD](ids, psName, isDense, rowNum, colNum)
    vertexRDD.updateValues(vertices)

    vertexRDD
  }
}

