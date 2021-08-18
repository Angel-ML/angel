package com.tencent.angel.graph.statistics.motif

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.model.neighbor.complex.ComplexNeighborModel
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.element.{NeighborTable, NeighborTablePartition}
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

class MotifCounting(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize with HasWeightCol
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasInput with HasExtraInputs
  with HasDelimiter with HasOutputTriangleCol with HasIsWeighted {

  def this() = this(Identifiable.randomUID("motif_counting_directed_v2"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Load data to edge rdd
    val edges = GraphIO.loadEdgesWithWeight(
      dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(weightCol), ${isWeighted}, false)
    println("num of edges " + edges.count())
    edges.persist(StorageLevel.DISK_ONLY)
    println(edges.take(1).mkString(","))

    // Build neighbor rdd and worker neighbor table
    println(s"build neighbor table partition")
    val neighborRDDForWorkerPS = MotifCounting.neighborAttr(edges, $(partitionNum))
    val neighborPartitions = NeighborTablePartition.fromNeighborTableRDD(neighborRDDForWorkerPS)
    neighborPartitions.persist($(storageLevel))

    // Get statics for sub-graph in task partition
    val stats = NeighborTablePartition.getStats[Float](neighborPartitions)
    println(s" calculate stats of graph, count the number of vertices and edges " + stats)

    // Start up Angel ps
    println(s" start parameter server")
    val psStartTime = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s" push neighbor tables to parameter server")
    val initTableStartTime = System.currentTimeMillis()
    val modelContext = new ModelContext(
      $(psPartitionNum), stats.minVertexId, stats.maxVertexId + 1, stats.numVertices,
      "neighbor", SparkContext.getOrCreate().hadoopConfiguration)
    val model = new ComplexNeighborModel(modelContext)
    model.init()

    // Release unused rdd: edges
    edges.unpersist()

    // Init graph on PS
    val neighborDataOps = new MotifNeighborOps(model)
    neighborDataOps.initLongNeighborAttrTag(neighborPartitions, $(batchSize))
    println(s"push costs ${System.currentTimeMillis() - initTableStartTime} ms")

    // Write checkpoint for model on PS
    val cpTableStartTime = System.currentTimeMillis()
    println(s"checkpoint neighbor table")
    model.checkpoint()
    println(s"checkpoint costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    // Calculate triangle count
    println(s"calculate motif")
    val resRdd = neighborDataOps.calWeightedMotif(neighborPartitions, $(pullBatchSize), ${isWeighted}).persist($(storageLevel))

    val motif = resRdd
      .reduceByKey(_ + _)
      .map(m => (m._1._1, (m._1._2, m._2)))
      .groupByKey()
      .map { case (id, iter) =>
        val libsvm = iter.toArray.sortBy(_._1).map(r => (r._1, r._2))
        var i = 1
        var j = 0
        val motifArr = new Array[Float](33)
        while (i <= 33) {
          if (j < libsvm.size && i == libsvm(j)._1) {
            motifArr(i - 1) = libsvm(j)._2
            j += 1
          } else {
            motifArr(i - 1) = 0
          }
          i += 1
        }
        (id, motifArr)
      }


    val output = motif.map { case (nodeId, tc) =>
      Row(nodeId, tc(0), tc(1), tc(2), tc(3), tc(4), tc(5), tc(6), tc(7), tc(8), tc(9), tc(10),
        tc(11), tc(12), tc(13), tc(14), tc(15), tc(16), tc(17), tc(18), tc(19), tc(20), tc(21),
        tc(22), tc(23), tc(24), tc(25), tc(26), tc(27), tc(28), tc(29), tc(30), tc(31), tc(32))
    }

    println(s"======sampled output======")
    output.take(50).sortBy(x => x.getLong(0)).foreach { row =>
      var i = 1
      var str = row.getLong(0).toString
      while (i <= 33) {
        if (row.getFloat(i) > 0) {
          str = str + "\t" + i + ":" + row.getFloat(i)
        }
        i += 1
      }
      println(str)
    }


    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(output, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField("motif-1", FloatType, nullable = false),
      StructField("motif-2", FloatType, nullable = false),
      StructField("motif-3", FloatType, nullable = false),
      StructField("motif-4", FloatType, nullable = false),
      StructField("motif-5", FloatType, nullable = false),
      StructField("motif-6", FloatType, nullable = false),
      StructField("motif-7", FloatType, nullable = false),
      StructField("motif-8", FloatType, nullable = false),
      StructField("motif-9", FloatType, nullable = false),
      StructField("motif-10", FloatType, nullable = false),
      StructField("motif-11", FloatType, nullable = false),
      StructField("motif-12", FloatType, nullable = false),
      StructField("motif-13", FloatType, nullable = false),
      StructField("motif-14", FloatType, nullable = false),
      StructField("motif-15", FloatType, nullable = false),
      StructField("motif-16", FloatType, nullable = false),
      StructField("motif-17", FloatType, nullable = false),
      StructField("motif-18", FloatType, nullable = false),
      StructField("motif-19", FloatType, nullable = false),
      StructField("motif-20", FloatType, nullable = false),
      StructField("motif-21", FloatType, nullable = false),
      StructField("motif-22", FloatType, nullable = false),
      StructField("motif-23", FloatType, nullable = false),
      StructField("motif-24", FloatType, nullable = false),
      StructField("motif-25", FloatType, nullable = false),
      StructField("motif-26", FloatType, nullable = false),
      StructField("motif-27", FloatType, nullable = false),
      StructField("motif-28", FloatType, nullable = false),
      StructField("motif-29", FloatType, nullable = false),
      StructField("motif-30", FloatType, nullable = false),
      StructField("motif-31", FloatType, nullable = false),
      StructField("motif-32", FloatType, nullable = false),
      StructField("motif-33", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}


object MotifCounting {

  def neighborAttr(edges: RDD[(Long, Long, Float)], partitionNum: Int): RDD[NeighborTable[Float]] = {

    // assume there is no redundant edges
    val edgeTag = edges.distinct().flatMap { case (src, dst, wgt) =>
      if (src == dst)
        Iterator.empty
      else if (src < dst) { // small node comes first, use tag to denote direction
        Iterator(((src, dst), (0, wgt))) // src -> dst, tag: 0
      } else {
        Iterator(((dst, src), (1, wgt))) // dst <- src, tag: 1
      }
    }.reduceByKey((a, b) => ((a._1 ^ b._1) + 1, a._2 + b._2), partitionNum)

    val neighborsForWorkerPS = edgeTag.flatMap { case ((srcId, dstId), (tag, wgt)) =>
      Iterator((srcId, (dstId, tag, wgt)), (dstId, (srcId, tag, wgt)))
    }.groupByKey().map {
      case (srcId, iter) =>
        val nbrs = ArrayBuffer[(Long, Byte, Float)]()
        iter.foreach { case (dstId, tag, wgt) =>
          nbrs.append((dstId, tag.toByte, wgt))
        }
        val (n, t, a) = nbrs.toArray.sortBy(_._1).unzip3
        new NeighborTable(srcId, n, t, a)
    }
    neighborsForWorkerPS
  }

}
