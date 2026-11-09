package com.tencent.angel.spark.ml.graph.algo.rank.intimacy

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

// calc intimacy value of one node and it's 2-nd order neighbors
// for directed graph

class Intimacy(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasPullBatchSize with HasBeta with HasTopK with HasBalancePartitionPercent {

  def this() = this(Identifiable.randomUID("2-nd order intimacy"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges
    val edges = Intimacy.loadEdges(dataset, $(srcNodeIdCol),
      $(dstNodeIdCol), $(isWeighted), $(weightCol))
    edges.persist(StorageLevel.DISK_ONLY)
    println(s"======sample edges======")
    println(edges.take(10).mkString(","))

    // push neighbor table to ps
    val neighborTable = Intimacy.edges2NeighborTable(edges, $(partitionNum))
    val (minId, maxId, numNodes, numEdges, maxDegree, minDegree) =
      Intimacy.stats(neighborTable)
    println(s"maxId: $maxId, minId: $minId, " +
      s"numNodes: $numNodes, numEdges: $numEdges, " +
      s"maxDegree: $maxDegree, " + s"minDegree: $minDegree")

    // Start PS and init the model
    println("start to run ps")
    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"Starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    println(s"push neighbor tables to ps")
    val initTableStartTime = System.currentTimeMillis()
    val psModel = IntimacyPSModel.fromMinMax(minId, maxId + 1, $(psPartitionNum), edges,
      $(useBalancePartition), $(storageLevel), $(balancePartitionPercent))
    psModel.initLongNeighborAttrs(neighborTable, $(batchSize))
    println(s"initializing the neighbor table costs ${System.currentTimeMillis()
      - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    psModel.psMatrix.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis()
      - cpTableStartTime} ms")

    // start calculating
    val result = neighborTable.mapPartitionsWithIndex { case (partId, iter) =>
      IntimacyOperator.runNeighborPartition_(partId, iter, psModel, $(pullBatchSize), $(topK))
    }

    val retRDD = result.map { case (itemI, intimacy) =>
      Row.fromSeq(Seq[Any](itemI, intimacy.map(x => x._1 + ":" + x._2).mkString(" ")))
    }

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        "node"
      }", LongType, nullable = false),
      StructField(s"${
        "intimacy"
      }", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object Intimacy {
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

  def edges2NeighborTable(edges: RDD[(Long, (Long, Float))],
                             partitionNum: Int): RDD[(Long, Array[(Long, Float)])] = {
    edges.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          Iterator.single(src, group.toArray.distinct)
        }
      } else {
        Iterator.empty
      }
    }
  }

  def stats(neighborTable: RDD[(Long, Array[(Long, Float)])]): (Long, Long, Long, Long,
    Long, Long) = {
    neighborTable.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      var numNodes = 0L
      var maxOutDegree = Long.MinValue
      var minOutDegree = Long.MaxValue
      iter.foreach { case(src, neighbors) =>
        maxOutDegree = math.max(maxOutDegree, neighbors.length)
        minOutDegree = math.min(minOutDegree, neighbors.length)
        min = math.min(min, math.min(src, neighbors.map(_._1).min))
        max = math.max(max, math.max(src, neighbors.map(_._1).max))
        numNodes += 1
        numEdges += neighbors.length
      }
      Iterator.single(( min, max, numNodes, numEdges, maxOutDegree, minOutDegree))
    }.reduce{ case (c1, c2) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 max c2._5,
        c1._6 min c2._6)
    }
  }
}
