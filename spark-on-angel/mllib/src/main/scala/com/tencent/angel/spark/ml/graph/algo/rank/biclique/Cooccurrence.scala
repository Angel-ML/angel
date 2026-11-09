package com.tencent.angel.spark.ml.graph.algo.rank.biclique

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.spark.ml.graph.algo.statistics.commonfriends.CommonFriendsPSModel
import org.apache.spark.rdd.RDD

// input: user->item pairs
// ${beta} => filter nodes with below beta occurrence times
// output is represented as 3 columns: user1, user2, co-occurrence times

class Cooccurrence(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasPullBatchSize with HasBeta {

  def this() = this(Identifiable.randomUID("co-occurrence"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges
    // user -> item pairs, no need to filter edges with the same id
    //      .filter(f => f._1 != f._2)
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))

    edges.persist(StorageLevel.DISK_ONLY)
    println(s"======sample edges======")
    println(edges.take(10).mkString(","))

    // push userItem neighbor table to ps
    val userItemNeighborTable = CooccurrenceOperator.userItem2NeighborTable(edges, $(partitionNum))
    val (minUserId, maxUserId, numUsers, numUserItemEdges, maxUserItemDegree, minUserItemDegree) = CooccurrenceOperator.stats(userItemNeighborTable)
    println(s"maxUserId: $maxUserId, minUserId: $minUserId, " +
      s"numUsers: $numUsers, numUserItemEdges: $numUserItemEdges, " +
      s"maxUserItemDegree: $maxUserItemDegree, " +
      s"minUserItemDegree: $minUserItemDegree")

    userItemNeighborTable.persist($(storageLevel))

    val itemUserNeighborTable = CooccurrenceOperator.userItem2NeighborTable(edges.map(e => (e._2, e._1)), $(partitionNum))
    val (minItemId, maxItemId, numItems, numItemUserEdges, maxItemUserDegree, minItemUserDegree) = CooccurrenceOperator.stats(itemUserNeighborTable)
    println(s"maxItemId: $maxItemId, minItemId: $minItemId, " +
      s"numItems: $numItems, numItemUserEdges: $numItemUserEdges, " +
      s"maxItemUserDegree: $maxItemUserDegree, " +
      s"minItemUserDegree: $minItemUserDegree")

    // Start PS and init the model
    println("start to run ps")
    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"Starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    println(s"push item-user neighbor tables to ps")
    val initTableStartTime = System.currentTimeMillis()
    val psModel = CommonFriendsPSModel(maxUserId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum),
      minUserId, useBalancePartition = $(useBalancePartition))
    psModel.initLongNeighborTable(userItemNeighborTable, edges.flatMap(x =>Iterator(x._1, x._2)))
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    // start calculating
    val result = itemUserNeighborTable.mapPartitionsWithIndex { case (partId, iter) =>
      CooccurrenceOperator.runNeighborPartition(partId, iter, psModel)
    }

    val retRDD = result.map { case (itemI, itemJ, coOccurrenceTimes) =>
      Row.fromSeq(Seq[Any](itemI, itemJ, coOccurrenceTimes))
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
        "coOccurrenceTimes"
      }", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object Cooccurrence {
  def loadEdges(dataset: Dataset[_], srcNodeIdCol: String, dstNodeIdCol: String,
                isWeighted: Boolean, weightIdCol: String): RDD[(Long, (Long, Float))] = {
    if (isWeighted) {
      dataset.select(srcNodeIdCol, dstNodeIdCol, weightIdCol).rdd.mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1) || row.getFloat(2) == 0)
            Iterator.empty
          else Iterator.single(row.getLong(0), (row.getLong(1), row.getFloat(2)))
        }
      }
    } else {
      dataset.select(srcNodeIdCol, dstNodeIdCol)}.rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (row.getLong(0) == row.getLong(1))
          Iterator.empty
        else
          Iterator.single(row.getLong(0), (row.getLong(1), 1.0f))
      }
    }
  }
}
