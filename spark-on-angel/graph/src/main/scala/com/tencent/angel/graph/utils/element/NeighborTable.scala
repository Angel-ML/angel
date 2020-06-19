package com.tencent.angel.graph.utils.element


import com.tencent.angel.graph.utils.element.Element.VertexId

import scala.reflect.ClassTag

case class NeighborTable[ED: ClassTag](
                                        var srcId: VertexId = -1,
                                        var neighborIds: Array[VertexId] = null,
                                        var attrs: Array[ED] = null
                                      ) extends Serializable {

  lazy val numEdges: Int = neighborIds.length

  def sorted(): this.type = {
    val neighbors = neighborIds.zip(attrs).sortBy(_._1)
    neighborIds = neighbors.map(_._1)
    attrs = neighbors.map(_._2)
    this
  }

  def withData[ED2: ClassTag](data: Array[ED2]): NeighborTable[ED2] = {
    NeighborTable(srcId, neighborIds, data).sorted()
  }

  def mapAttrs[ED2: ClassTag](f: ED => ED2): NeighborTable[ED2] = {
    val newAttrs = attrs.map(f)
    this.withData(newAttrs)
  }

  def map[ED2: ClassTag](f: NeighborTable[ED] => ED2): NeighborTable[ED2] = {
    val newAttrs = new Array[ED2](numEdges)
    var i = 0
    while (i < numEdges) {
      newAttrs(i) = f(this)
      i += 1
    }
    this.withData(newAttrs)
  }

  def updateValues(newAttrs: Array[ED]): Unit = {
    this.attrs = newAttrs
  }

  override def toString: String = {
    if (attrs == null || attrs.isEmpty)
      s"src = $srcId, neighbors = ${neighborIds.mkString(",")}"
    else
      s"src = $srcId, neighbors = ${neighborIds.mkString(",")}, edge attrs = ${attrs.mkString(",")}"
  }
}
