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

/**
 * A single directed edge consisting of a source id, target id,
 * and the data associated with the edge.
 *
 * @tparam ED type of the edge attribute
 *
 * @param srcId The vertex id of the source vertex
 * @param dstId The vertex id of the target vertex
 * @param attr The attribute associated with the edge
 */
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED](
  var srcId: VertexId = 0,
  var dstId: VertexId = 0,
  var attr: ED = null.asInstanceOf[ED])
  extends Serializable {

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge.
   * @return the id of the other vertex on the edge.
   */
  def otherVertexId(vid: VertexId): VertexId =
    if (srcId == vid) {
      dstId
    } else {
      assert(dstId == vid)
      srcId
    }

  /**
   * Return the relative direction of the edge to the corresponding
   * vertex.
   *
   * @param vid the id of one of the two vertices in the edge.
   * @return the relative direction of the edge to the corresponding
   *         vertex.
   */
  def relativeDirection(vid: VertexId): EdgeDirection =
    if (vid == srcId) {
      EdgeDirection.Out
    } else {
      assert(vid == dstId)
      EdgeDirection.In
    }

  private[graphx] def toEdgeTriplet[VD: ClassTag](srcAttr: VD, dstAttr: VD): EdgeTriplet[VD, ED] = {
    val et = new EdgeTriplet[VD, ED]
    et.srcId = srcId
    et.srcAttr = srcAttr
    et.dstId = dstId
    et.dstAttr = dstAttr
    et.attr = attr
    et
  }
}
