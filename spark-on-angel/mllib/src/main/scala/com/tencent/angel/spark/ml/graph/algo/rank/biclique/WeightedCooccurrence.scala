package com.tencent.angel.spark.ml.graph.algo.rank.biclique

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.model.neighbor.complex.ComplexNeighborModel
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.data.neighbor.{NeighborTable, NodeNeighborPartition, NodeNeighborsOps}
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.graph.utils.params._
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

// input: user->item pairs
// ${beta} => filter nodes with below beta occurrence times
// output is represented as 3 columns: user1, user2, co-occurrence times

class WeightedCooccurrence(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasPullBatchSize with HasBeta {

  def this() = this(Identifiable.randomUID("weighted co-occurrence"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = GraphIO.loadEdgesWithWeight(dataset, $(srcNodeIdCol), $(dstNodeIdCol),
      $(weightCol), $(isWeighted), false)
    edges.persist(StorageLevel.DISK_ONLY)
    println(s"======sample edges======")
    println(edges.take(10).mkString(","))

    // push userItem neighbor table to ps
    val userItemNeighborTable = WeightedCooccurrence.userItem2NeighborTable(
      edges.map(e => (e._1, (e._2, e._3))), $(partitionNum))
    val userItemNeighborParts = NodeNeighborPartition.fromNeighborTableRDD[Float](userItemNeighborTable)
    val stats = NodeNeighborPartition.getStats(userItemNeighborParts)
    println(s"maxUserId: ${stats.maxVertexId}, minUserId: ${stats.minVertexId}, " +
      s"numUsers: ${stats.numVertices}, numUserItemEdges: ${stats.numEdges}")

    userItemNeighborTable.persist($(storageLevel))

    val itemUserNeighborTable = WeightedCooccurrence.itemUser2NeighborTable(
      edges.map(e => (e._2, (e._1, e._3))), $(partitionNum))
    val (minId, maxId, numNodes, numEdges, maxOutDegree, minOutDegree) =
      WeightedCooccurrence.stats(itemUserNeighborTable)

    // Start PS and init the model
    println("start to run ps")
    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"Starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    println(s"push user-item neighbor tables to ps")
    val initTableStartTime = System.currentTimeMillis()
    val modelContext = new ModelContext(
      $(psPartitionNum), minId, maxId + 1, numNodes, "neighbor", SparkContext.getOrCreate().hadoopConfiguration)
    val model = new ComplexNeighborModel(modelContext)

    // Check use load balance partition
    // TODO: remove later
    if ($(useBalancePartition) && !modelContext.isUseHashPartition) {
      println(s"use ps balance partition strategy.")
      val mc = new MatrixContext()
      val index = edges.flatMap(e => Iterator(e._1, e._2))
      LoadBalancePartitioner.partition(index, $(psPartitionNum), mc)
      model.init(mc)
    } else {
      model.init()
    }

    // Release unused rdd: edges
    edges.unpersist()

    val neighborDataOps = new NodeNeighborsOps(model)
    neighborDataOps.initLongNeighborAttrTag(userItemNeighborParts, $(batchSize))
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    model.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    // start calculating
    val result = itemUserNeighborTable.mapPartitionsWithIndex { case (partId, iter) =>
      CooccurrenceOperator.runWeightedNeighborPartition(partId, iter, model, $(batchSize))
    }

    val retRDD = result.map { case (itemI, itemJ, coOccurrenceValue) =>
      Row.fromSeq(Seq[Any](itemI, itemJ, coOccurrenceValue))
    }

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        "itemI"
      }", LongType, nullable = false),
      StructField(s"${
        "itemJ"
      }", LongType, nullable = false),
      StructField(s"${
        "coOccurrence"
      }", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object WeightedCooccurrence {
  def userItem2NeighborTable(edges: RDD[(Long, (Long, Float))],
                             partitionNum: Int): RDD[NeighborTable[Float]] = {
    edges.groupByKey(partitionNum).mapPartitionsWithIndex { (partId, iter) =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          val nbrs = new Array[Long](group.size)
          val attr = new Array[Float](group.size)
          // to do: tag or attr could be null
          val tags = new Array[Byte](group.size)
          group.zipWithIndex.foreach { case ((nei, weight), index) =>
            nbrs(index) = nei
            attr(index) = weight
            tags(index) = 0.toByte
          }
          Iterator.single(new NeighborTable[Float](src, nbrs, tags, attrs = attr))
        }
      } else {
        Iterator.empty
      }
    }
  }

  def itemUser2NeighborTable(edges: RDD[(Long, (Long, Float))],
                             partitionNum: Int): RDD[(Long, Array[(Long, Float)])] = {
    edges.groupByKey(partitionNum).mapPartitionsWithIndex { (partId, iter) =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(src, group.toArray.sortBy(_._1))
        }
      } else {
        Iterator.empty
      }
    }
  }

  def stats(neighborTable: RDD[(Long, Array[(Long, Float)])]): (Long, Long, Long, Long, Long, Long) = {
    neighborTable.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      var numNodes = 0L
      var maxOutDegree = Long.MinValue
      var minOutDegree = Long.MaxValue
      iter.foreach { case (src, neighbors) =>
        maxOutDegree = math.max(maxOutDegree, neighbors.length)
        minOutDegree = math.min(minOutDegree, neighbors.length)
        min = math.min(min, src)
        max = math.max(max, src)
        numNodes += 1
        numEdges += neighbors.length
      }
      Iterator.single((min, max, numNodes, numEdges, maxOutDegree, minOutDegree))
    }.reduce { case (c1, c2) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 max c2._5, c1._6 min c2._6)
    }
  }
}
