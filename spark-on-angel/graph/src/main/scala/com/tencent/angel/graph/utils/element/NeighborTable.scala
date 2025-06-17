package com.tencent.angel.graph.utils.element


import com.tencent.angel.graph.utils.element.Element.VertexId

import scala.reflect.ClassTag

case class NeighborTable[ED: ClassTag](
                                        var srcId: VertexId = -1,
                                        var neighborIds: Array[VertexId] = null,
                                        var tags: Array[Byte] = null,
                                        var attrs: Array[ED] = null
                                      ) extends Serializable {
  lazy val numEdges: Int = neighborIds.length

  def sorted(): this.type = {
    if (tags != null) {
      val neighbors = neighborIds.zip(tags).zip(attrs).sortBy(_._1._1)
      neighborIds = neighbors.map(_._1._1)
      tags = neighbors.map(_._1._2)
      attrs = neighbors.map(_._2)
    } else {
      val neighbors = neighborIds.zip(attrs).sortBy(_._1)
      neighborIds = neighbors.map(_._1)
      attrs = neighbors.map(_._2)
    }

    this
  }

  def withData[ED2: ClassTag](data: Array[ED2]): NeighborTable[ED2] = {
    NeighborTable[ED2](srcId, neighborIds, tags, data).sorted()
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
    if (attrs == null || tags == null || attrs.isEmpty)
      s"src = $srcId, neighbors = ${neighborIds.mkString(",")}"
    else if (attrs == null && tags != null)
      s"src = $srcId, neighbors = ${neighborIds.mkString(",")}, edge tags = ${tags.mkString(",")}"
    else if (tags == null && attrs != null)
      s"src = $srcId, neighbors = ${neighborIds.mkString(",")}, edge attrs = ${attrs.mkString(",")},"
    else
      s"src = $srcId, neighbors = ${neighborIds.mkString(",")}, edge attrs = ${attrs.mkString(",")}, edge tags = ${tags.mkString(",")}"
  }
}
