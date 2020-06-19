package com.tencent.angel.graph.utils.element

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
