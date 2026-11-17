package com.tencent.angel.spark.ml.graph.data.neighbor

import com.tencent.angel.graph.model.neighbor.complex.{ComplexNeighborModel, NeighborsAttrTagElement}
import com.tencent.angel.spark.ml.graph.algo.statistics.triangle.MotifNeighborModel
import com.tencent.angel.spark.ml.graph.data.neighbor.NodeNeighborPartition._
import com.tencent.angel.spark.ml.graph.data.{CheckMotif, GraphStats, PartitionId, VertexId}
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import it.unimi.dsi.fastutil.longs.{Long2ObjectArrayMap, Long2ObjectOpenHashMap}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class NodeNeighborPartition[@specialized(Byte, Boolean, Short, Int, Long, Float, Double) ED: ClassTag](
                                                                                                        isDirected: Boolean,
                                                                                                        var partitionID: PartitionId,
                                                                                                        var srcIds: Array[VertexId],
                                                                                                        var neighbors: Array[Array[VertexId]],
                                                                                                        var edgeAttrs: Array[Array[ED]],
                                                                                                        var edgeTags: Array[Array[Byte]]
                                                                                                      ) extends Serializable {
  private def this(isDirected: Boolean) = this(isDirected, -1, null, null, null, null)

  private def this() = this(false)

  lazy val size: Int = srcIds.length

  lazy val numVertices: Long = srcIds.length

  lazy val numEdges: Long = neighbors.map(_.length).sum

  lazy val stats: GraphStats = {
    var minVertex = Long.MaxValue
    var maxVertex = Long.MinValue
    (0 until numVertices.toInt).foreach { pos =>
      minVertex = minVertex min srcIds(pos) min neighbors(pos).head
      maxVertex = maxVertex max srcIds(pos) max neighbors(pos).last
    }
    GraphStats(minVertex, maxVertex, numVertices, numEdges)
  }

  private def fromNeighborRDD(data: Iterator[NodeNeighbors[ED]]): this.type = {
    val localSrcIds = new ArrayBuffer[VertexId]()
    val localNeighbors = new ArrayBuffer[Array[VertexId]]
    val localAttrs = new ArrayBuffer[Array[ED]]
    val localTags = new ArrayBuffer[Array[Byte]]

    data.foreach { nt =>
      localSrcIds += nt.srcId
      localNeighbors += nt.neighbors
      localAttrs += nt.attrs
      localTags += nt.tags
    }
    this
  }

  def takeBatch(from: Int, length: Int): Array[NodeNeighbors[ED]] = {
    val length2 = length min (size - from)
    val rec = new Array[NodeNeighbors[ED]](length2)
    (0 until length2).foreach { pos =>
      rec(pos) = NodeNeighbors(srcIds(from + pos),
        neighbors(from + pos),
        if (edgeAttrs == null) null else edgeAttrs(from + pos), if (edgeTags == null) null else edgeTags(from + pos))
    }
    rec
  }

  def iterator: Iterator[NodeNeighbors[ED]] = new Iterator[NodeNeighbors[ED]] {
    private[this] val instance = new NodeNeighbors[ED]
    private[this] var pos = 0

    override def hasNext: Boolean = pos < NodeNeighborPartition.this.size

    override def next(): NodeNeighbors[ED] = {
      instance.srcId = srcIds(pos)
      instance.neighbors = neighbors(pos)
      instance.attrs = if (edgeAttrs == null || edgeAttrs.isEmpty) null else edgeAttrs(pos)
      instance.tags = if (edgeTags == null || edgeTags.isEmpty) null else edgeTags(pos)
      pos += 1
      instance
    }
  }


  def batchIterator(batchSize: Int): Iterator[Array[NodeNeighbors[ED]]] =
    BatchIter(iterator, batchSize)

  def makeBatchIterator(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0

    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, NodeNeighborPartition.this.size))
    }

    override def hasNext: Boolean = {
      index < NodeNeighborPartition.this.size
    }
  }

  def calMotifDirectedV2(model: ComplexNeighborModel, batchSize:Int, isWeighted: Boolean = false,
                         threshold: Int = Int.MaxValue) = {
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()

      val pullNodes = new mutable.HashSet[Long]()
      val srcNodes = new Array[Long](to - from)
      (from until to).foreach { pos =>
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
        srcNodes(pos - from) = srcIds(pos)
      }
      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = model.getNeighbors(pullNodes.toArray)
      totalRowNum += (to - from)
      totalPullNum += pullNodes.size
      println(s"partition $partitionID: process ${to - from} neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcEle = psNbrTable.get(src)
        val srcNbrs = srcEle.getNeighborIds
        val srcTags = srcEle.getTags
        val srcAttrs = srcEle.getAttrs
        val validIndex = srcNbrs.indices.filter(srcNbrs(_) > src)
        if (validIndex.length > threshold)
          Iterator.single((src, 0.toByte), -1f)
        else
          validIndex.flatMap { index =>
            val dst = srcNbrs(index)
            val tag = srcTags(index)
            val attr = srcAttrs(index)
            val dstEle = psNbrTable.get(dst)
            if (dstEle != null && dstEle.getNodesNum > 0) {
              val commNbrs = intersect(srcEle, dstEle).toMap
              computeMotifCountV2(src, dst, (tag, attr), srcEle, dstEle, commNbrs, isWeighted)
            } else {
              Iterator.empty
            }
          }
      }
    }
  }

  def calGroupMotifDirectedV2(model: MotifNeighborModel, batchSize:Int, isWeighted: Boolean = false,
                              threshold: Int = Int.MaxValue) = {
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var computeStartTs = 0L

    println(s"partition $partitionID: #vertices: ${stats.numVertices}, #edges: ${stats.numEdges}")
    makeBatchIterator(batchSize).flatMap { case (from, to) =>
      val endTs = System.currentTimeMillis()
      println(s"partition $partitionID: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()

      val pullNodes = new mutable.HashSet[Long]()
      val srcNodes = new Array[Long](to - from)
      (from until to).foreach { pos =>
        pullNodes ++= neighbors(pos)
        pullNodes.add(srcIds(pos))
        srcNodes(pos - from) = srcIds(pos)
      }
      val beforePullTs = System.currentTimeMillis()
      val psNbrTable = model.getNeighbors(pullNodes.toArray)
      val temp = psNbrTable.values().iterator()

      // second neighbors' tags must be obtained
      while (temp.hasNext) {
        pullNodes ++= temp.next().getNeighborIds
      }
      val psCommunities = model.readTag(pullNodes.toArray)
      totalRowNum += (to - from)
      totalPullNum += pullNodes.size
      println(s"partition $partitionID: process ${to - from} neighbor tables ($totalRowNum in total), " +
        s"pull neighbors of ${pullNodes.size} nodes from PS ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      computeStartTs = System.currentTimeMillis()
      srcNodes.flatMap { src =>
        val srcEle = psNbrTable.get(src)
        val srcNbrs = srcEle.getNeighborIds
        val srcTags = srcEle.getTags
        val srcAttrs = srcEle.getAttrs
        val srcCommunities = psCommunities.get(src)
        val validIndex = srcNbrs.indices.filter(srcNbrs(_) > src)
        if (validIndex.length > threshold)
          Iterator.single((src, 0.toByte), -1f)
        else
          validIndex.flatMap { index =>
            val dst = srcNbrs(index)
            val tag = srcTags(index)
            val attr = srcAttrs(index)
            val dstEle = psNbrTable.get(dst)
            val dstCommunities = psCommunities.get(dst)
            val validCommunities = intersect(srcCommunities.toArray.sorted, dstCommunities.toArray.sorted)
            if (dstEle != null && dstEle.getNodesNum > 0 && validCommunities.length > 0) {
              val commNbrs = intersect(srcEle, dstEle).toMap
              computeGroupMotifCountV2(src, dst, (tag, attr), srcEle, dstEle, commNbrs, validCommunities, psCommunities, isWeighted)
            } else {
              Iterator.empty
            }
          }
      }
    }
  }

}

object NodeNeighborPartition {

  def fromNeighborTableRDD[ED: ClassTag](data: RDD[NeighborTable[ED]], isDirected: Boolean = false): RDD[NodeNeighborPartition[ED]] = {
    data.mapPartitionsWithIndex { case (partId, iter) =>
      val localSrcs = new ArrayBuffer[VertexId]
      val localNeighbors = new ArrayBuffer[Array[VertexId]]
      val localAttrs = new ArrayBuffer[Array[ED]]
      val localTags = new ArrayBuffer[Array[Byte]]
      iter.foreach { item =>
        localSrcs += item.srcId
        localNeighbors += item.neighborIds
        if (item.attrs != null) {
          localAttrs += item.attrs
        }
        if (item.tags != null) {
          localTags += item.tags
        }
      }
      Iterator.single(new NodeNeighborPartition[ED](isDirected,
        partId,
        localSrcs.toArray,
        localNeighbors.toArray,
        if (localAttrs.isEmpty) null else localAttrs.toArray,
        if (localTags.isEmpty) null else localTags.toArray))
    }
  }

  def getStats[ED: ClassTag](data: RDD[NodeNeighborPartition[ED]]): GraphStats = {
    data.map(_.stats).reduce(_ + _)
  }

  def intersect(src: NeighborsAttrTagElement, dst: NeighborsAttrTagElement): Array[(Long, ((Byte, Float), (Byte, Float)))] = {

    val res = new ArrayBuffer[(Long, ((Byte, Float), (Byte, Float)))]
    if (src == null || dst == null || src.getNodesNum == 0 || dst.getNodesNum == 0)
      return Array()

    var i = 0
    var j = 0
    val srcNbrs = src.getNeighborIds
    val srcTags = src.getTags
    val srcAttrs = src.getAttrs
    val dstNbrs = dst.getNeighborIds
    val dstTags = dst.getTags
    val dstAttrs = dst.getAttrs

    while (i < srcNbrs.length && j < dstNbrs.length) {
      if (srcNbrs(i) < dstNbrs(j)) i += 1
      else if (srcNbrs(i) > dstNbrs(j)) j += 1
      else {
        res += ((srcNbrs(i), ((srcTags(i), srcAttrs(i)), (dstTags(j), dstAttrs(j)))))
        i += 1
        j += 1
      }
    }
    res.toArray
  }

  def intersect(src: Array[Long], dst: Array[Long]): Array[Long] = {
    val res = new ArrayBuffer[Long]()
    if (src == null || dst == null)
      return Array()
    var i = 0
    var j = 0
    while (i < src.length && j < dst.length) {
      if (src(i) < dst(j)) i += 1
      else if (src(i) > dst(j)) j += 1
      else {
        res.append(src(i))
        i += 1
        j += 1
      }
    }
    res.toArray
  }

  def computeMotifCountV2(src: Long, dst: Long, tag: (Byte, Float),
                          srcNbrs: NeighborsAttrTagElement, dstNbrs: NeighborsAttrTagElement,
                          commonNbrs: Map[Long, ((Byte, Float), (Byte, Float))],
                          isWeighted: Boolean=false): Array[((Long, Byte), Float)] = {
    val motifCount = new ArrayBuffer[((Long, Byte), Float)]() // (node,motifType)

    //motif type of this edge
    val (edgeMotifSrc, edgeMotifDst) = if (tag._1 == 0) (1, 2) else if (tag._1 == 1) (2, 1) else (14, 14)
    if (isWeighted) {
      motifCount.append(((src, edgeMotifSrc.toByte), tag._2))
      motifCount.append(((dst, edgeMotifDst.toByte), tag._2))
    } else {
      motifCount.append(((src, edgeMotifSrc.toByte), 1.0f))
      motifCount.append(((dst, edgeMotifDst.toByte), 1.0f))
    }

    //motif type of triangle: (src,dst)and common neighbors
    if (commonNbrs.size > 0) {
      for ((node, (src2nodeTag, dst2nodeTag)) <- commonNbrs) {
        var (srcIn, srcOut) = (0, 0)
        var (dstIn, dstOut) = (0, 0)
        var (nodeIn, nodeOut) = (0, 0)
        //only count triangle-motif only the smallest edge
        if (node > math.max(src, dst) && src < dst) {
          //judge the motif type of this triangle
          tag._1 match {
            case 0 => srcOut += 1; dstIn += 1
            case 1 => srcIn += 1; dstOut += 1
            case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
          }

          src2nodeTag._1 match {
            case 0 => srcOut += 1; nodeIn += 1
            case 1 => srcIn += 1; nodeOut += 1
            case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
          }

          dst2nodeTag._1 match {
            case 0 => dstOut += 1; nodeIn += 1
            case 1 => dstIn += 1; nodeOut += 1
            case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
          }

          val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
          val motifTag = motifTriangle.get(degSeq).get
          //          println(src, dst, node, degSeq,motifTag.mkString(","))
          val intensity = if (isWeighted) intensityScore3(tag._2, src2nodeTag._2, dst2nodeTag._2) else 1.0f

          val vArr = Array(src, dst, node)
          for (t <- motifTag.indices) {
            if (motifTag(t) != 0) {
              motifCount.append(((vArr(t), motifTag(t)), intensity))
            }
          }
        }
      } // triangle-motif has been count
    }

    // now count twoEdge-motif,first count (src,dst) and srcNbrs

    def filterSrcView(eleNbrs: Array[Long], commonNbrs: Map[Long, ((Byte, Float), (Byte, Float))], src: Long, dst: Long) = {
      val filterMinView = new ArrayBuffer[Int]()
      val filterMaxView = new ArrayBuffer[Int]()

      for (idx <- eleNbrs.indices) {
        if (!commonNbrs.contains(eleNbrs(idx))) {
          if (src < math.min(dst, eleNbrs(idx)) && dst < eleNbrs(idx))
            filterMinView.append(idx)
          else if (dst > math.max(src, eleNbrs(idx)))
            filterMaxView.append(idx)
        }
      }
      (filterMinView.toArray, filterMaxView.toArray)
    }

    def filterDstView(eleNbrs: Array[Long], commonNbrs: Map[Long, ((Byte, Float), (Byte, Float))], src: Long, dst: Long) = {
      val filterMinView = new ArrayBuffer[Int]()
      val filterMaxView = new ArrayBuffer[Int]()

      for (idx <- eleNbrs.indices) {
        if (!commonNbrs.contains(eleNbrs(idx))) {
          if (src < dst && src < math.min(dst, eleNbrs(idx)))
            filterMinView.append(idx)
          else if (src > eleNbrs(idx) && dst > math.max(src, eleNbrs(idx)))
            filterMaxView.append(idx)
        }
      }
      (filterMinView.toArray, filterMaxView.toArray)
    }

    if (srcNbrs.getNodesNum >= 1) {
      val srcNbrs_nodes = srcNbrs.getNeighborIds
      val srcNbrs_attrs = srcNbrs.getAttrs
      val srcNbrs_tags = srcNbrs.getTags
      val (filterMinView, filterMaxView) = filterSrcView(srcNbrs_nodes, commonNbrs, src, dst)

      var i = 0
      while (i <= 2) {
        val validIdx = filterMinView.filter(srcNbrs_tags(_) == i)
        if (validIdx.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotifV2(src, dst, srcNbrs_nodes(validIdx(0)), tag, i.toByte)
          var m = 0f
          if (isWeighted) validIdx.foreach { idx => m += intensityScore2(tag._2, srcNbrs_attrs(idx)) } else m = validIdx.length.toFloat
          motifCount.append(((src, srcMotif), m))
          motifCount.append(((dst, dstMotif), m))
        }

        val nbrsMaxSmall = filterMaxView.filter { idx => srcNbrs_tags(idx) == i && srcNbrs_nodes(idx) < src }
        val nbrsMaxBig = filterMaxView.filter { idx => srcNbrs_tags(idx) == i && srcNbrs_nodes(idx) > src }
        if (nbrsMaxSmall.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotifV2(src, dst, srcNbrs_nodes(nbrsMaxSmall(0)), tag, i.toByte)
          var m = 0f
          if (isWeighted) nbrsMaxSmall.foreach { idx => m += intensityScore2(tag._2, srcNbrs_attrs(idx)) } else m = nbrsMaxSmall.length.toFloat
          motifCount.append(((dst, nodeMotif), m))
        }

        if (nbrsMaxBig.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotifV2(src, dst, srcNbrs_nodes(nbrsMaxBig(0)), tag, i.toByte)
          var m = 0f
          if (isWeighted) nbrsMaxBig.foreach { idx => m += intensityScore2(tag._2, srcNbrs_attrs(idx)) } else m = nbrsMaxBig.length.toFloat
          motifCount.append(((dst, dstMotif), m))
        }
        i += 1
      }
    }

    if (dstNbrs.getNodesNum >= 1) {
      val dstNbrs_nodes = dstNbrs.getNeighborIds
      val dstNbrs_attrs = dstNbrs.getAttrs
      val dstNbrs_tags = dstNbrs.getTags
      val (filterMinView, filterMaxView) = filterDstView(dstNbrs_nodes, commonNbrs, src, dst)

      var i = 0
      while (i <= 2) { // the tag of the edge (dst,dstNbr)
        val nbrSmall = filterMinView.filter { idx => dstNbrs_tags(idx) == i && dstNbrs_nodes(idx) < dst }
        val nbrBig = filterMinView.filter { idx => dstNbrs_tags(idx) == i && dstNbrs_nodes(idx) > dst }

        val nbrMax = filterMaxView.filter { idx => dstNbrs_tags(idx) == i }

        if (nbrSmall.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotifV2(src, dst, dstNbrs_nodes(nbrSmall(0)), tag, i.toByte)
          var m = 0f
          if (isWeighted) nbrSmall.foreach { case idx => m += intensityScore2(tag._2, dstNbrs_attrs(idx)) } else m = nbrSmall.length.toFloat
          motifCount.append(((src, srcMotif), m))
          motifCount.append(((dst, dstMotif), m))
        }

        if (nbrBig.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotifV2(src, dst, dstNbrs_nodes(nbrBig(0)), tag, i.toByte)
          var m = 0f
          if (isWeighted) nbrBig.foreach { idx => m += intensityScore2(tag._2, dstNbrs_attrs(idx)) } else m = nbrBig.length.toFloat
          motifCount.append(((src, srcMotif), m))
          motifCount.append(((dst, dstMotif), m))
        }

        if (nbrMax.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotifV2(src, dst, dstNbrs_nodes(nbrMax(0)), tag, i.toByte)
          var m = 0f
          if (isWeighted) nbrMax.foreach { case idx => m += intensityScore2(tag._2, dstNbrs_attrs(idx)) } else m = nbrMax.length.toFloat
          motifCount.append(((src, nodeMotif), m))
        }
        i += 1
      }
    }
    motifCount.toArray
  }

  def computeGroupMotifCountV2(src: Long, dst: Long, tag: (Byte, Float),
                          srcNbrs: NeighborsAttrTagElement, dstNbrs: NeighborsAttrTagElement,
                          commonNbrs: Map[Long, ((Byte, Float), (Byte, Float))],
                               validCommunities: Array[Long],
                               psCommunities: Long2ObjectOpenHashMap[Set[Long]],
                          isWeighted: Boolean=false): Array[((Long, Byte), Float)] = {
    val motifCount = new ArrayBuffer[((Long, Byte), Float)]() // (node,motifType)

    //motif type of this edge
    val (edgeMotifSrc, edgeMotifDst) = if (tag._1 == 0) (1, 2) else if (tag._1 == 1) (2, 1) else (14, 14)
    val num = if (isWeighted) tag._2 else 1.0f
    for (community <- validCommunities) {
      motifCount.append(((community, edgeMotifSrc.toByte), num))
      motifCount.append(((community, edgeMotifDst.toByte), num))
    }

    //motif type of triangle: (src,dst)and common neighbors
    if (commonNbrs.size > 0) {
      for ((node, (src2nodeTag, dst2nodeTag)) <- commonNbrs) {
        val nodeTags = psCommunities.get(node).toArray.sorted
        val validTag = intersect(validCommunities, nodeTags)
        if (validTag.length > 0) {
          var (srcIn, srcOut) = (0, 0)
          var (dstIn, dstOut) = (0, 0)
          var (nodeIn, nodeOut) = (0, 0)
          //only count triangle-motif only the smallest edge
          if (node > math.max(src, dst) && src < dst) {
            //judge the motif type of this triangle
            tag._1 match {
              case 0 => srcOut += 1; dstIn += 1
              case 1 => srcIn += 1; dstOut += 1
              case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
            }

            src2nodeTag._1 match {
              case 0 => srcOut += 1; nodeIn += 1
              case 1 => srcIn += 1; nodeOut += 1
              case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
            }

            dst2nodeTag._1 match {
              case 0 => dstOut += 1; nodeIn += 1
              case 1 => dstIn += 1; nodeOut += 1
              case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
            }

            val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
            val motifTag = motifTriangle.get(degSeq).get
            //          println(src, dst, node, degSeq,motifTag.mkString(","))
            val intensity = if (isWeighted) intensityScore3(tag._2, src2nodeTag._2, dst2nodeTag._2) else 1.0f

            for (tag <- validTag) {
//              val vArr = Array(src, dst, node)
              for (t <- motifTag.indices) {
                if (motifTag(t) != 0) {
                  motifCount.append(((tag, motifTag(t)), intensity))
                }
              }
            }
          }
        }
      } // triangle-motif has been count
    }

    // now count twoEdge-motif,first count (src,dst) and srcNbrs

    def filterSrcView(eleNbrs: Array[Long], commonNbrs: Map[Long, ((Byte, Float), (Byte, Float))], src: Long, dst: Long) = {
      val filterMinView = new ArrayBuffer[Int]()
      val filterMaxView = new ArrayBuffer[Int]()

      for (idx <- eleNbrs.indices) {
        if (!commonNbrs.contains(eleNbrs(idx))) {
          if (src < math.min(dst, eleNbrs(idx)) && dst < eleNbrs(idx))
            filterMinView.append(idx)
          else if (dst > math.max(src, eleNbrs(idx)))
            filterMaxView.append(idx)
        }
      }
      (filterMinView.toArray, filterMaxView.toArray)
    }

    def filterDstView(eleNbrs: Array[Long], commonNbrs: Map[Long, ((Byte, Float), (Byte, Float))], src: Long, dst: Long) = {
      val filterMinView = new ArrayBuffer[Int]()
      val filterMaxView = new ArrayBuffer[Int]()

      for (idx <- eleNbrs.indices) {
        if (!commonNbrs.contains(eleNbrs(idx))) {
          if (src < dst && src < math.min(dst, eleNbrs(idx)))
            filterMinView.append(idx)
          else if (src > eleNbrs(idx) && dst > math.max(src, eleNbrs(idx)))
            filterMaxView.append(idx)
        }
      }
      (filterMinView.toArray, filterMaxView.toArray)
    }

    if (srcNbrs.getNodesNum >= 1) {
      val srcNbrs_nodes = srcNbrs.getNeighborIds
      val srcNbrs_attrs = srcNbrs.getAttrs
      val srcNbrs_tags = srcNbrs.getTags
      val (filterMinView, filterMaxView) = filterSrcView(srcNbrs_nodes, commonNbrs, src, dst)

      var i = 0
      while (i <= 2) {
        val validIdx = filterMinView.filter(srcNbrs_tags(_) == i)
        if (validIdx.length > 0) {
          for (validTag <- validCommunities) {
            val trueIdx = validIdx.filter(x => psCommunities.get(srcNbrs_nodes(x)).contains(validTag))
            if (trueIdx.length > 0) {
              val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotifV2(src, dst, srcNbrs_nodes(validIdx(0)), tag, i.toByte)
              var m = 0f
              if (isWeighted) trueIdx.foreach { idx => m += intensityScore2(tag._2, srcNbrs_attrs(idx)) } else m = trueIdx.length.toFloat
              motifCount.append(((validTag, srcMotif), m))
              motifCount.append(((validTag, dstMotif), m))
            }
          }
        }

        val nbrsMaxSmall = filterMaxView.filter { idx => srcNbrs_tags(idx) == i && srcNbrs_nodes(idx) < src }
        val nbrsMaxBig = filterMaxView.filter { idx => srcNbrs_tags(idx) == i && srcNbrs_nodes(idx) > src }
        if (nbrsMaxSmall.length > 0) {
          for (validTag <- validCommunities) {
            val trueIdx = nbrsMaxSmall.filter(x => psCommunities.get(srcNbrs_nodes(x)).contains(validTag))
            if (trueIdx.length > 0) {
              val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotifV2(src, dst, srcNbrs_nodes(nbrsMaxSmall(0)), tag, i.toByte)
              var m = 0f
              if (isWeighted) trueIdx.foreach { idx => m += intensityScore2(tag._2, srcNbrs_attrs(idx)) } else m = trueIdx.length.toFloat
              motifCount.append(((validTag, nodeMotif), m))
            }
          }
        }

        if (nbrsMaxBig.length > 0) {
          for (validTag <- validCommunities) {
            val trueIdx = nbrsMaxBig.filter(x => psCommunities.get(srcNbrs_nodes(x)).contains(validTag))
            if (trueIdx.length > 0) {
              val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotifV2(src, dst, srcNbrs_nodes(nbrsMaxBig(0)), tag, i.toByte)
              var m = 0f
              if (isWeighted) trueIdx.foreach { idx => m += intensityScore2(tag._2, srcNbrs_attrs(idx)) } else m = trueIdx.length.toFloat
              motifCount.append(((validTag, dstMotif), m))
            }
          }
        }
        i += 1
      }
    }

    if (dstNbrs.getNodesNum >= 1) {
      val dstNbrs_nodes = dstNbrs.getNeighborIds
      val dstNbrs_attrs = dstNbrs.getAttrs
      val dstNbrs_tags = dstNbrs.getTags
      val (filterMinView, filterMaxView) = filterDstView(dstNbrs_nodes, commonNbrs, src, dst)

      var i = 0
      while (i <= 2) { // the tag of the edge (dst,dstNbr)
        val nbrSmall = filterMinView.filter { idx => dstNbrs_tags(idx) == i && dstNbrs_nodes(idx) < dst }
        val nbrBig = filterMinView.filter { idx => dstNbrs_tags(idx) == i && dstNbrs_nodes(idx) > dst }

        val nbrMax = filterMaxView.filter { idx => dstNbrs_tags(idx) == i }

        if (nbrSmall.length > 0) {
          for (validTag <- validCommunities) {
            val trueIdx = nbrSmall.filter(x => psCommunities.get(dstNbrs_nodes(x)).contains(validTag))
            if (trueIdx.length > 0) {
              val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotifV2(src, dst, dstNbrs_nodes(nbrSmall(0)), tag, i.toByte)
              var m = 0f
              if (isWeighted) trueIdx.foreach { case idx => m += intensityScore2(tag._2, dstNbrs_attrs(idx)) } else m = trueIdx.length.toFloat
              motifCount.append(((validTag, srcMotif), m))
              motifCount.append(((validTag, dstMotif), m))
            }
          }
        }

        if (nbrBig.length > 0) {
          for (validTag <- validCommunities) {
            val trueIdx = nbrBig.filter(x => psCommunities.get(dstNbrs_nodes(x)).contains(validTag))
            if (trueIdx.length > 0) {
              val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotifV2(src, dst, dstNbrs_nodes(nbrBig(0)), tag, i.toByte)
              var m = 0f
              if (isWeighted) trueIdx.foreach { idx => m += intensityScore2(tag._2, dstNbrs_attrs(idx)) } else m = trueIdx.length.toFloat
              motifCount.append(((validTag, srcMotif), m))
              motifCount.append(((validTag, dstMotif), m))
            }
          }
        }

        if (nbrMax.length > 0) {
          for (validTag <- validCommunities) {
            val trueIdx = nbrMax.filter(x => psCommunities.get(dstNbrs_nodes(x)).contains(validTag))
            if (trueIdx.length > 0) {
              val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotifV2(src, dst, dstNbrs_nodes(nbrMax(0)), tag, i.toByte)
              var m = 0f
              if (isWeighted) trueIdx.foreach { case idx => m += intensityScore2(tag._2, dstNbrs_attrs(idx)) } else m = trueIdx.length.toFloat
              motifCount.append(((validTag, nodeMotif), m))
            }
          }
        }
        i += 1
      }
    }
    motifCount.toArray
  }


  def motifTriangle = motifMatrix.toMap

  def motifEdge = motifTwoEdge

  def twoEdge = twoEdgeMatrix

  def intensityScore3(x: Float, y: Float, z: Float) = math.pow(x * y * z, 1f / 3).toFloat

  def intensityScore2(x: Float, y: Float) = math.pow(x * y, 1f / 2).toFloat

  def motifMatrix = {
    val matrix = collection.mutable.Map[String, Array[Byte]]()
    matrix.put("200211", Array(11, 12, 13))
    matrix.put("201102", Array(11, 13, 12))
    matrix.put("022011", Array(12, 11, 13))
    matrix.put("021120", Array(12, 13, 11))
    matrix.put("112002", Array(13, 11, 12))
    matrix.put("110220", Array(13, 12, 11))

    matrix.put("111111", Array(10, 10, 10))

    matrix.put("222222", Array(33, 33, 33))

    matrix.put("022121", Array(23, 24, 24))
    matrix.put("210221", Array(24, 23, 24))
    matrix.put("212102", Array(24, 24, 23))

    matrix.put("201212", Array(25, 26, 26))
    matrix.put("122012", Array(26, 25, 26))
    matrix.put("121220", Array(26, 26, 25))

    matrix.put("121121", Array(27, 28, 29))
    matrix.put("122111", Array(27, 29, 28))
    matrix.put("111221", Array(28, 27, 29))
    matrix.put("112112", Array(28, 29, 27))
    matrix.put("211112", Array(29, 28, 27))
    matrix.put("211211", Array(29, 27, 28))

    matrix.put("122221", Array(30, 31, 32))
    matrix.put("122122", Array(30, 32, 31))
    matrix.put("221221", Array(31, 30, 32))
    matrix.put("222112", Array(31, 32, 30))
    matrix.put("212212", Array(32, 31, 30))
    matrix.put("211222", Array(32, 30, 31))

    matrix.put("222222", Array(33, 33, 33))
    matrix.toMap
  }


  def twoEdgeMatrix = {
    val matrix = collection.mutable.Map[String, Byte]()

    matrix.put("011011", 3)
    matrix.put("011110", 3)
    matrix.put("110110", 3)
    matrix.put("111001", 3)
    matrix.put("101101", 3)
    matrix.put("100111", 3)

    matrix.put("010120", 5)
    matrix.put("012001", 5)
    matrix.put("200101", 5)

    matrix.put("101002", 4)
    matrix.put("100210", 4)
    matrix.put("021010", 4)

    matrix.put("111210", 6)
    matrix.put("111012", 6)
    matrix.put("101112", 6)
    matrix.put("101211", 6)
    matrix.put("121011", 6)
    matrix.put("121110", 6)

    matrix.put("110121", 7)
    matrix.put("112101", 7)
    matrix.put("211101", 7)
    matrix.put("210111", 7)
    matrix.put("012111", 7)
    matrix.put("011121", 7)

    matrix.put("221111", 8)
    matrix.put("112211", 8)
    matrix.put("111122", 8)
    matrix.toMap
  }

  def motifTwoEdge = {
    val matrix = collection.mutable.Map[String, Array[Byte]]()

    matrix.put("011011", Array(3, 5, 4))
    matrix.put("011110", Array(3, 4, 5))
    matrix.put("110110", Array(4, 3, 5))
    matrix.put("111001", Array(4, 5, 3))
    matrix.put("101101", Array(5, 4, 3))
    matrix.put("100111", Array(5, 3, 4))

    matrix.put("010120", Array(7, 7, 6))
    matrix.put("012001", Array(7, 6, 7))
    matrix.put("200101", Array(6, 7, 7))

    matrix.put("101002", Array(9, 9, 8))
    matrix.put("100210", Array(9, 8, 9))
    matrix.put("021010", Array(8, 9, 9))

    matrix.put("111210", Array(15, 16, 17))
    matrix.put("111012", Array(15, 17, 16))
    matrix.put("101112", Array(17, 15, 16))
    matrix.put("101211", Array(17, 16, 15))
    matrix.put("121011", Array(16, 17, 15))
    matrix.put("121110", Array(16, 15, 17))

    matrix.put("110121", Array(18, 20, 19))
    matrix.put("112101", Array(18, 19, 20))
    matrix.put("211101", Array(19, 18, 20))
    matrix.put("210111", Array(19, 20, 18))
    matrix.put("012111", Array(20, 19, 18))
    matrix.put("011121", Array(20, 18, 19))
    matrix.put("221111", Array(21, 22, 22))
    matrix.put("112211", Array(22, 21, 22))
    matrix.put("111122", Array(22, 22, 21))
    matrix.toMap
  }
}
