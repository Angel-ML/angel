package com.tencent.angel.spark.ml.graph.data.neighbor

import com.tencent.angel.spark.ml.graph.data.VertexId

import scala.reflect.ClassTag


case class NodeNeighbors[ED: ClassTag](
                                        var srcId: VertexId = -1,
                                        var neighbors: Array[VertexId] = null,
                                        var attrs: Array[ED] = null,
                                        var tags: Array[Byte] = null
                                      ) extends Serializable {

  lazy val numEdges: Int = neighbors.length

  /**
    * sorted neighbors by neighbor's vertexId
    *
    * @return
    */
  def sorted(): this.type = {
    val neighborsAttr = neighbors.zip(attrs).zip(tags).sortBy(_._1._1)
    neighbors = neighborsAttr.map(_._1._1)
    attrs = neighborsAttr.map(_._1._2)
    tags = neighborsAttr.map(_._2)
    this
  }

  /**
    * change neighbor's data
    *
    * @param data
    * @tparam ED2
    * @return
    */
  def withData[ED2: ClassTag](data: Array[ED2]): NodeNeighbors[ED2] = {
    NodeNeighbors(srcId, neighbors, data, tags)
  }


  /**
    * change neighbor's data
    *
    * @param f
    * @tparam ED2
    * @return
    */
  def mapAttrs[ED2: ClassTag](f: ED => ED2): NodeNeighbors[ED2] = {
    val newAtts = attrs.map(f)
    this.withData(newAtts)
  }

  def map[ED2: ClassTag](f: NodeNeighbors[ED] => ED2): NodeNeighbors[ED2] = {
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
      s"src = $srcId, neighbors = ${neighbors.mkString(",")}"
    else if (tags == null)
      s"src = $srcId, neighbors = ${neighbors.mkString(",")}, edge attrs = ${attrs.mkString(",")}"
    else
      s"src = $srcId, neighbors = ${neighbors.mkString(",")}, edge attrs = ${attrs.mkString(",")} tags = ${tags.mkString(",")}"
  }
}
