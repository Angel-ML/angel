package com.tencent.angel.graph.community.egonetwork

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.model.neighbor.dynamic.DynamicNeighborModel
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.graph.utils.Stats.{summarizeApplyOp, summarizeReduceOp}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, Long2ObjectOpenHashMap, LongArrayList}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class EgoNetworkV2(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasNodeAttrPath with HasPullBatchSize
  with HasExtraInputs with HasNeedReplicaEdge {

  def this() = this(Identifiable.randomUID("EgoTriangleEdges"))

  private var approxNumNodes: Long = -1L
  def setNumNodes(in: Long): Unit = { this.approxNumNodes = in }

  private var compressEdges: Boolean = false
  def setCompressEdges(in: Boolean): Unit = { this.compressEdges = in }

  var nodeForEgo: RDD[Long] = _

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1))
            Iterator.empty
          else
            Iterator.single(row.getLong(0), row.getLong(1)) }
      }

    edges.persist(StorageLevel.DISK_ONLY)
    println(s"======sample edges======")
    println(edges.take(10).mkString(","))

    val beforeStats = System.currentTimeMillis()
    val (minId, maxId, numEdges) = edges.mapPartitions(summarizeApplyOp).reduce(summarizeReduceOp)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")
    println(s"stats cost ${System.currentTimeMillis() - beforeStats} ms.")

    // Start PS and init the model
    println("start to run ps")
    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"Starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    println(s"push neighbor tables to ps")
    val initTableStartTime = System.currentTimeMillis()
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId+1L, approxNumNodes,
      "dynamic_neighbor", dataset.sparkSession.sparkContext.hadoopConfiguration)
    val psModel = new DynamicNeighborModel(modelContext)
    psModel.init()

    val initNumEdges = psModel.initNeighbors(edges, $(batchSize), needReplica = $(needReplicaEdge))
    println(s"init $initNumEdges edges.")
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")

    println(s"start pulling nodes.")
    val beforePullNodesTime = System.currentTimeMillis()
    val nodes = psModel.getNodes($(psPartitionNum)).repartition($(partitionNum)).persist($(storageLevel))
    println(s"pulled ${nodes.count()} nodes, cost ${System.currentTimeMillis() - beforePullNodesTime} ms.")

    val beforeTransTime = System.currentTimeMillis()
    println(s"start get and sort processing.")
    val transNum = psModel.trans(nodes, $(pullBatchSize) * 10)
    println(s"processed $transNum nodes with dynamic neighbors, cost ${System.currentTimeMillis() - beforeTransTime} ms.")

    val cpTableStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    val result = if (nodeForEgo != null) {
      println(s"calculate triangle edges for nodes from path: ${$(extraInputs)(0)}, " +
        s"numNodes=${nodeForEgo.count()}")

      nodeForEgo.repartition($(partitionNum)).mapPartitionsWithIndex { case (partId, iter) =>
        EgoNetworkV2.runNodePartition(partId, iter, psModel, $(pullBatchSize))
      }
    } else {
      println(s"calculate triangle edges for all nodes.")
      nodes.mapPartitionsWithIndex { case (partId, iter) =>
        EgoNetworkV2.runNodePartition(partId, iter, psModel, $(pullBatchSize))
      }
    }

    dataset.sparkSession.createDataFrame(result, transformSchema(dataset.schema))
  }

  def setNodeForEgo(data: RDD[Long]): Unit = { nodeForEgo = data }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${"node"}", LongType, nullable = false),
      StructField(s"nodes", ArrayType(LongType), nullable = false),
      StructField(s"${"edges"}", ArrayType(ArrayType(LongType)), nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object EgoNetworkV2 {

  def runNodePartition(partId: Int, iter: Iterator[Long], psModel: DynamicNeighborModel,
                       batchSize: Int, compressEdges: Boolean=false): Iterator[Row] = {
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var beforePullTs = System.currentTimeMillis()
      val firstNeighborTable = psModel.getNeighbors(batchIter)
      val firstPullTime = System.currentTimeMillis() - beforePullTs

      val secondPullNodes = new mutable.HashSet[Long]()
      val temp = firstNeighborTable.values().iterator()
      while (temp.hasNext) { secondPullNodes ++= temp.next() }
      beforePullTs = System.currentTimeMillis()
      if (secondPullNodes.nonEmpty) {
        val psNeighborTable = psModel.getNeighbors(secondPullNodes.toArray)
        val secondPullTime = System.currentTimeMillis() - beforePullTs
        // get ps neighbor table splits
        val validIndices = new Long2IntOpenHashMap(psNeighborTable.size())
        val t = psNeighborTable.long2ObjectEntrySet().fastIterator()
        val beforeSplit = System.currentTimeMillis()
        while (t.hasNext) {
          val data = t.next()
          val node = data.getLongKey
          val neis = data.getValue
          validIndices.put(node, find(neis, node))
        }
        val splitTime = System.currentTimeMillis() - beforeSplit

        println(s"partition $partId: process ${batchIter.length} nodes, " +
          s"firstSize=${batchIter.length}, firstPullTime=$firstPullTime, " +
          s"secondSize=${secondPullNodes.size}, secondPullTime=$secondPullTime, " +
          s"splitTime=$splitTime")

        computeStartTs = System.currentTimeMillis()
        batchIter.flatMap { src =>
          val firstNeighbor = firstNeighborTable.get(src)
          val res = new Long2ObjectOpenHashMap[LongArrayList]()
          val nodes = new mutable.HashSet[Long]()
          firstNeighbor.foreach { nei =>
            intersect(firstNeighbor, psNeighborTable.get(nei), validIndices.get(nei), nei, res)
          }
          val re = res.keySet().toLongArray().flatMap { k =>
            nodes.add(k)
            res.get(k).toLongArray().map { x =>
              nodes.add(x)
              Array(k, x)}
          }
          Iterator.single(Row(src, nodes.toArray, re))
        }
      } else {
        Iterator.empty
      }
    }
  }

  def find(arr: Array[Long], value: Long): Int = {
    var i = 0
    while (i < arr.length && arr(i) <= value )
      i += 1
    i
  }

  def intersect(array1: Array[Long], array2: Array[Long], start: Int, nei: Long, res: Long2ObjectOpenHashMap[LongArrayList]): Unit = {
    if (array1 != null && array2 != null && start < array2.length) {
      val temp = res.get(nei)
      if (temp == null) {
        val t = new LongArrayList()
        intersect(array1, array2, start, t)
        if (!t.isEmpty) res.put(nei, t)
      } else {
        intersect(array1, array2, start, temp)
      }
    }
  }

  def intersect(array1: Array[Long], array2: Array[Long], start: Int, res: LongArrayList): Unit = {
    if (array1 != null && array2 != null && start < array2.length) {
      var i = 0
      var j = start
      while (i < array1.length && j < array2.length) {
        if (array1(i) < array2(j))
          i += 1
        else if (array1(i) > array2(j))
          j += 1
        else {
          res.add(array1(i))
          i += 1
          j += 1
        }
      }
    }
  }
}
