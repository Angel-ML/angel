package com.tencent.angel.spark.ml.graph.algo.statistics.triangle

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.model.neighbor.simple.SimpleNeighborTableModel
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.algo.statistics.lccUndirected.GroupLccUndirected
import com.tencent.angel.spark.ml.graph.utils.collection.{OpenHashMap, OpenHashSet}
import com.tencent.angel.spark.ml.graph.utils.{GraphIO, Stats}
import com.tencent.angel.spark.ml.graph.utils.params._
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GroupDirectedTriangle(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize with HasWeightCol
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasInput with HasExtraInputs
  with HasDelimiter with HasOutputTriangleCol with HasIsWeighted {

  def this() = this(Identifiable.randomUID("groupDirectedTriangleCounting"))

  var userGroup: RDD[(Long, Long)] = _
  def setUserGroup(in: RDD[(Long, Long)]): Unit = { this.userGroup = in }

  var hasEdgeType: Boolean = false
  def setHasEdgeType(in: Boolean): Unit = { this.hasEdgeType = in }

  var outputDir: String = _
  def setOutputDir(in: String): Unit = { this.outputDir = in }

  override def transform(dataset: Dataset[_]): DataFrame = {
    // get stats of all edges
    val (minId, maxId, numEdges) = GroupDirectedTriangle.getStats(dataset)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    // Start up Angel ps firstly
    println(s"start parameter server")
    val psStartTime = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    // create user-group ps matrix
    val tagContext = new ModelContext($(psPartitionNum),  minId, maxId+1, -1, "tag", SparkContext.getOrCreate().hadoopConfiguration)
    val model = new SimpleNeighborTableModel(tagContext)
    model.init()

    // init user-group on ps
    val beforeInitTag = System.currentTimeMillis()
    userGroup.groupByKey($(partitionNum)).map(x => (x._1, x._2.toArray.distinct.sorted))
      .mapPartitions { iter => iter.sliding($(batchSize), $(batchSize))
        .map(pairs => model.initNeighbors(pairs))
      }.count()
    println(s"push tag cost ${System.currentTimeMillis() - beforeInitTag} ms")

    val cpTableStartTime = System.currentTimeMillis()
    model.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    // calc by edge types
    val edgeTypes: Array[Long] = if (hasEdgeType) GroupDirectedTriangle.getEdgeTypes(dataset) else Array()
    if (edgeTypes.isEmpty) {
      val edges = GroupDirectedTriangle.loadEdges(dataset)
      val edges_ = { if (edges.getNumPartitions >= $(partitionNum))
        edges.coalesce($(partitionNum))
      else
        edges.repartition($(partitionNum))
      }.mapPartitionsWithIndex { case (index, iter) =>
        GroupLccUndirected.filterEdges(index, iter, $(pullBatchSize), model)
      }
      val result = calc(edges_, $(partitionNum), model)
      println(s"======sample results======")
      result.take(10).foreach { case (groupId, numNodesInGroup, numEdgesInGroup,triangles) =>
        println(s"groupId=$groupId, numNodesInGroup=$numNodesInGroup, " +
          s"numEdgesInGroup=$numEdgesInGroup,triangles=${triangles.mkString(" ")}")
      }
      // save
      val outputSchema = transformSchema(dataset.schema)
      val re = result.map { case (groupId, numNodesInGroup, numEdgesInGroup,triangles) =>
        Row(groupId, numNodesInGroup, numEdgesInGroup, triangles(0), triangles(1), triangles(2), triangles(3), triangles(4), triangles(5), triangles(6))
      }
      val df = dataset.sparkSession.createDataFrame(re, outputSchema)
      GraphIO.save(df, outputDir)
    } else {
      // filter and flat edges by groups
      val rawEdges = GroupDirectedTriangle.loadEdgesWithType(dataset)
      val filtered = { if (rawEdges.getNumPartitions >= ${partitionNum})
        rawEdges.coalesce($(partitionNum))
      else
        rawEdges.repartition($(partitionNum))
      }.mapPartitionsWithIndex { case (index, iter) =>
        GroupDirectedTriangle.filterEdges(index, iter, $(pullBatchSize), model)
      }.persist($(storageLevel))
      println(s"num flattened edges: ${filtered.count()}")
      println(s"num edgeTypes=${edgeTypes.length}")
      for (index <- edgeTypes.indices) {
        println(s"------ edge type ${edgeTypes(index)} ------")
        val edges = filtered.filter(_._2 == edgeTypes(index)).map(x => (x._1, x._3))
        val result = calc(edges, $(partitionNum), model)
        println(s"======sample results======")
        result.take(10).foreach { case (groupId, numNodesInGroup, numEdgesInGroup, triangles) =>
          println(s"groupId=$groupId, edgeType=${edgeTypes(index)}, numNodesInGroup=$numNodesInGroup, " +
            s"numEdgesInGroup=$numEdgesInGroup, triangles=${triangles.mkString(" ")}")
        }
        // save
        val outputSchema = transformSchema(dataset.schema)
        val re = result.map { case (groupId, numNodesInGroup, numEdgesInGroup, triangles) =>
          Row(groupId, edgeTypes(index), numNodesInGroup, numEdgesInGroup, triangles(0), triangles(1), triangles(2), triangles(3), triangles(4), triangles(5), triangles(6))
        }
        val df = dataset.sparkSession.createDataFrame(re, outputSchema)
        if (index == 0) GraphIO.save(df, outputDir) else GraphIO.appendSave(df, outputDir)
      }
    }

    // return
    val t = SparkContext.getOrCreate().parallelize(List("1", "2"), 1)
    val schema = StructType(Seq(StructField("temp", StringType, nullable = false)))
    dataset.sparkSession.createDataFrame(t.map(x => Row(x)), schema)
  }

  override def transformSchema(schema: StructType): StructType = {
    val seq = new ArrayBuffer[StructField]()
    seq.append(StructField("groupId", LongType, nullable = false))
    if (hasEdgeType) seq.append(StructField("edgeType", LongType, nullable = false))
    seq.append(StructField("numNodesInGroup", IntegerType, nullable = false))
    seq.append(StructField("numEdgesInGroup", IntegerType, nullable = false))
    for (i <- 1 to 7) {
      seq.append(StructField("triangleType-"+i, IntegerType, nullable = false))
    }
    StructType(seq)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def calc(edges: RDD[(Long, (Long, Long))], partitionNum: Int, model: SimpleNeighborTableModel) = {
    // calc directed triangles by group
    edges.groupByKey(partitionNum).mapPartitionsWithIndex { case (index, iter) =>
      var startTs = System.currentTimeMillis()
      iter.sliding(1000, 1000).flatMap { batchIter =>
        val endTs = System.currentTimeMillis()
        println(s"calc group directed triangles: partition $index: last batch total_time: ${endTs - startTs} ms.")
        startTs = System.currentTimeMillis()
        batchIter.flatMap { case (groupId, data) =>
          val edges = data.toArray
          val nodesInGroup = new OpenHashSet[Long]()
          edges.foreach { case (src, dst) =>
            nodesInGroup.add(src)
            nodesInGroup.add(dst)
          }
          val numNodesInGroup = nodesInGroup.size
          val numEdgesInGroup = edges.length
          val triangles = GroupDirectedTriangle.calcDirectedTriangle(edges)
          Iterator.single(groupId, numNodesInGroup, numEdgesInGroup, triangles)
        }
      }
    }
  }

}


object GroupDirectedTriangle {

  def getStats(dataset: Dataset[_]): (Long, Long, Long) = {
    val edges = dataset.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)
    Stats.summarize(edges)
  }

  def loadEdges(dataset: Dataset[_]): RDD[(Long, Long)] = {
    dataset.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)
  }

  def loadEdgesWithType(dataset: Dataset[_]): RDD[(Long, Long, Long)] = {
    dataset.select("src", "dst", "edgeType").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1), row.getLong(2)))
      .filter(f => f._1 != f._2)
  }

  def getEdgeTypes(dataset: Dataset[_]): Array[Long] = {
    dataset.select("edgeType").rdd
      .filter(!_.anyNull)
      .map(x => (x.getLong(0), 1))
      .reduceByKey(_ + _)
      .map(_._1)
      .collect()
  }

  def intersect(src: Array[(Long, Int)], dst: Array[(Long, Int)]): Array[(Long, Int, Int)] = {
    if (src == null || src.length == 0 || dst == null || dst.length == 0)
      Array[(Long, Int, Int)]()
    else {
      val count = new ArrayBuffer[(Long, Int, Int)]()
      var i = 0
      var j = 0
      while (i < src.length && j < dst.length) {
        if (src(i)._1 < dst(j)._1) i += 1
        else if (src(i)._1 > dst(j)._1) j += 1
        else {
          count.append((src(i)._1, src(i)._2, dst(j)._2))
          i += 1
          j += 1
        }
      }
      count.toArray
    }
  }

  def intersect(src: Array[Long], dst: Array[Long]): Array[Long] = {
    if (src == null || src.length == 0 || dst == null || dst.length == 0)
      Array[Long]()
    else {
      val count = new ArrayBuffer[Long]()
      var i = 0
      var j = 0
      while (i < src.length && j < dst.length) {
        if (src(i) < dst(j)) i += 1
        else if (src(i) > dst(j)) j += 1
        else {
          count.append(src(i))
          i += 1
          j += 1
        }
      }
      count.toArray
    }
  }

  def triangleType(in: (Int, Int, Int)): Int = {
    var numBiDirectionalEdges = 0
    if (in._1 == 2) numBiDirectionalEdges += 1
    if (in._2 == 2) numBiDirectionalEdges += 1
    if (in._3 == 2) numBiDirectionalEdges += 1

    numBiDirectionalEdges match {
      case 0 =>
        in match {
          case (0,1,0) => 1 // cycle
          case _ => 0 // trans
        }
      case 1 =>
        in match {
          case (0,0,2) => 2 // out-recip
          case (2,1,1) => 2 // out-recip
          case (0,2,1) => 4 // in-recip
          case (2,0,0) => 4 // in-recip
          case _ => 3 // trans-recip
        }
      case 2 => 5 // two-recip
      case _ => 6 // numBiDirectionalEdges=3, three-recip
    }
  }

  def calcDirectedTriangle(edges: Iterable[(Long, Long)]): Array[Int] = {
    var before = System.currentTimeMillis()
    val neighborTable = getNeighborTable(edges)
//    println(s"calc neighbor table cost ${System.currentTimeMillis() - before} ms.")
    before = System.currentTimeMillis()
    val triangleCount = new Array[Int](7)
    neighborTable.keySet().toLongArray().foreach { src =>
      val srcNbrs = neighborTable.get(src)
      srcNbrs.foreach { case (dst, tag0) =>
        val dstNbrs = neighborTable.get(dst)
        val commons = intersect(srcNbrs, dstNbrs)
        if (commons.nonEmpty) {
          commons.foreach { case (_, tag1, tag2) => triangleCount(triangleType((tag0, tag1, tag2))) += 1 }
        }
      }
    }
//    println(s"compute triangle cost ${System.currentTimeMillis() - before} ms.")
    triangleCount
  }

  def getNeighborTable(edges: Iterable[(Long, Long)]) = {
    val neighborTable = new Long2ObjectOpenHashMap[OpenHashMap[Long, Int]]()
    edges.foreach { case (src, dst) =>
      val (tag, min, max) = if (src < dst) (0, src, dst) else (1, dst, src)
      val data = neighborTable.get(min)
      if (data == null) {
        val tmp = new OpenHashMap[Long, Int]()
        tmp.update(max, tag)
        neighborTable.put(min, tmp)
      } else {
        data.changeValue(max, tag, x => x ^ tag + 1)
      }
    }
    val re = new Long2ObjectOpenHashMap[Array[(Long, Int)]](neighborTable.size())
    neighborTable.keySet().toLongArray().foreach { src =>
      val tmp = neighborTable.get(src).toArray.sortBy(_._1)
      re.put(src, tmp)
    }
    neighborTable.clear()
    re
  }

  def filterEdges(index: Int, iter: Iterator[(Long, Long, Long)], batchSize: Int, model: SimpleNeighborTableModel): Iterator[(Long, Long, (Long, Long))] = {
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()
    iter.sliding(batchSize, batchSize).flatMap { batchIter =>
      val endTs = System.currentTimeMillis()
      println(s"filter edges: partition $index: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes = new mutable.HashSet[Long]()
      batchIter.foreach { case (src, dst, _) =>
        pullNodes.add(src)
        pullNodes.add(dst)
      }
      val beforePullTs = System.currentTimeMillis()
      val pulledTags = model.getNeighbors(pullNodes.toArray)
      println(s"pulled ${pullNodes.size} tag, cost ${System.currentTimeMillis() - beforePullTs} ms.")
      computeStartTs = System.currentTimeMillis()
      batchIter.flatMap { case (src, dst, edgeType) =>
        val commons = intersect(pulledTags.get(src), pulledTags.get(dst))
        if (commons.nonEmpty) {
          commons.map(groupId => (groupId, edgeType, (src, dst))).toIterator
        } else Iterator.empty
      }
    }
  }

}
