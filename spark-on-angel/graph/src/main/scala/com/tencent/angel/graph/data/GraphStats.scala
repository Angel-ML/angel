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
package com.tencent.angel.graph.data

case class GraphStats(
                       var minVertexId: Long,
                       var maxVertexId: Long,
                       var numVertices: Long,
                       var numEdges: Long
                     ) extends Serializable {

  def +(other: GraphStats): GraphStats = {
    GraphStats(
      this.minVertexId min other.minVertexId,
      this.maxVertexId max other.maxVertexId,
      if (this.numVertices != -1 && other.numVertices != -1)
        this.numVertices + other.numVertices
      else -1,
      this.numEdges + other.numEdges
    )
  }

  override def toString: String = {
    s"min vertex id = ${minVertexId}, " +
      s"max vertex id = ${maxVertexId}, " +
      s"num vertices = ${numVertices}, " +
      s"num edges = ${numEdges}"
  }
}
