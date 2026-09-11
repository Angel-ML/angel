package com.tencent.angel.graph.community.egonetwork

import com.tencent.angel.graph.statistics.commonfriends.CommonFriendsPSModel
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class EgoNetwork(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasNodeAttrPath with HasPullBatchSize
  with HasExtraInputs {

  def this() = this(Identifiable.randomUID("EgoNetwork"))

  var nodeForEgo: RDD[Long] = _
  var maxNodeId: Long = _
  var minNodeId: Long = _

  override def transform(dataset: Dataset[_]): DataFrame = {
    //
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1))
            Iterator.empty
          else
            Iterator((row.getLong(0), row.getLong(1)), (row.getLong(1), row.getLong(0))) }
      }

    edges.persist(StorageLevel.DISK_ONLY)
    println(s"======sample edges======")
    println(edges.take(10).mkString(","))


    // push userItem neighbor table to ps
    val userNeighborTable = EgoOperator.edge2NeighborTable(edges, $(partitionNum))
    val (minId, maxId, numNodes, numEdges, maxDegree) = EgoNetwork.stats(userNeighborTable)
        println(s"maxId: $maxId, minId: $minId, " +
          s"numNodes: $numNodes, numEdges: $numEdges, " +
          s"maxDegree: $maxDegree")
    minNodeId = minId
    maxNodeId = maxId

    userNeighborTable.persist($(storageLevel))

    // Start PS and init the model
    println("start to run ps")
    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"Starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    println(s"push neighbor tables to ps")
    val initTableStartTime = System.currentTimeMillis()
    val psModel = CommonFriendsPSModel(maxId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum),
      minIndex = minId)
    psModel.initLongNeighborTable(userNeighborTable, edges.flatMap(x =>Iterator(x._1, x._2)))
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    val result = if (nodeForEgo != null) {
      println(s"calculate ego networks according to nodes from path: ${$(extraInputs)(0)}")
      nodeForEgo.repartition($(partitionNum)).mapPartitionsWithIndex { case (partId, iter) =>
        EgoOperator.runNodePartition(partId, iter, psModel)
      }
    } else {
      println(s"calculate ego networks for all nodes.")
      userNeighborTable.mapPartitionsWithIndex { case (partId, iter) =>
        EgoOperator.runNeighborPartition(partId, iter, psModel)
      }
    }

    val retRDD = result.map { case (node, neighbors, edges) =>
      Row.fromSeq(Seq[Any](node, neighbors, edges))
    }

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))

  }

  def setNodeForEgo(data: RDD[Long]): Unit = {
    nodeForEgo = data.filter(x => x <= maxNodeId).filter(x => x >= minNodeId)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        "node"
      }", LongType, nullable = false),
      StructField(s"${
        "neighbors"
      }", StringType, nullable = false),
      StructField(s"${
        "edges"
      }", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object EgoNetwork {
  def stats(neighborTable: RDD[(Long, Array[Long])]): (Long, Long, Long, Long, Long) = {
    neighborTable.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      var numNodes = 0L
      var maxOutDegree = Long.MinValue
      iter.foreach { case (src, neighbors) =>
        maxOutDegree = math.max(maxOutDegree, neighbors.length)
        min = math.min(min, src)
        max = math.max(max, src)
        numNodes += 1
        numEdges += neighbors.length
      }
      Iterator.single((min, max, numNodes, numEdges, maxOutDegree))
    }.reduce { case (c1, c2) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 max c2._5)
    }
  }
}
