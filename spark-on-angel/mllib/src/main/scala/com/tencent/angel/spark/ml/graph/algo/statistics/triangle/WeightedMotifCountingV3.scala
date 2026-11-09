package com.tencent.angel.spark.ml.graph.algo.statistics.triangle

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.model.neighbor.complex.ComplexNeighborModel
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.data.neighbor.{NeighborTable, NodeNeighborPartition, NodeNeighborsOps}
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

// same as WeightedMotifCountingV2, except that V3 has a threshold for super nodes
// which would be cut and processed by edge partitions after dealing with the plain nodes

class WeightedMotifCountingV3(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize with HasWeightCol
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasSuperItemThreshold
  with HasInput with HasExtraInputs with HasDelimiter with HasOutputTriangleCol
  with HasIsWeighted {

  def this() = this(Identifiable.randomUID("motif_counting_directed_v3"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Load data to edge rdd
    val edges = GraphIO.loadEdgesWithWeight(
      dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(weightCol), $(isWeighted), false)
    println("num of edges " + edges.count())
    edges.persist(StorageLevel.DISK_ONLY)
    println(edges.take(1).mkString(","))

    println(s"build neighbor table partition")
    val neighborRDDForWorkerPS = WeightedMotifCountingV3.neighborAttr(edges, $(partitionNum))
    val neighborPartitions = NodeNeighborPartition.fromNeighborTableRDD(neighborRDDForWorkerPS)
    neighborPartitions.persist($(storageLevel))

    val stats = NodeNeighborPartition.getStats[Float](neighborPartitions)
    println(s" calculate stats of graph, count the number of vertices and edges " + stats)

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
    val neighborDataOps = new NodeNeighborsOps(model)
    neighborDataOps.initLongNeighborAttrTag(neighborPartitions, $(batchSize))
    println(s"push costs ${System.currentTimeMillis() - initTableStartTime} ms")

    val cpTableStartTime = System.currentTimeMillis()
    println(s"checkpoint neighbor table")
    model.checkpoint()
    println(s"checkpoint costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    println(s"calculate motif")
    val resRdd = neighborDataOps.calWeightedMotifV3(neighborPartitions, $(superItemThreshold)
      , $(pullBatchSize), $(isWeighted)).persist($(storageLevel))
    val plainRdd = resRdd.filter(_._2 > 0).reduceByKey(_ + _).persist($(storageLevel))
    println(s"finished calculating plain nodes, got ${plainRdd.count()} records.")

    // ------------- calc superNodes ---------------
    val superNodes = resRdd.filter(_._2 < 0).map(_._1._1).persist(StorageLevel.MEMORY_ONLY)
    val numSuperNodes = superNodes.count()
    resRdd.unpersist()
    println(s"num super nodes: $numSuperNodes")

    // recover super edges and process by edge cut
    val superRdd = WeightedMotifCountingV3.calcSuperRdd(numSuperNodes, superNodes, model,
      $(partitionNum), $(storageLevel), $(pullBatchSize), $(isWeighted))
    superRdd.persist($(storageLevel))

    val motif = superRdd.reduceByKey(_ + _).union(plainRdd)
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


object WeightedMotifCountingV3 {

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

  def calcSuperRdd(numSuperNodes: Long, superNodes: RDD[Long], model: ComplexNeighborModel,
                   partitionNum: Int, storageLevel: StorageLevel, batchSize: Int,
                   isWeighted: Boolean = false) = {
    if (numSuperNodes > 0) {
      println(s"start calculating super nodes...")
      val edges = superNodes.mapPartitions { iter =>
        if (iter.nonEmpty) {
          val nodes = iter.toArray
          val before = System.currentTimeMillis()
          val neisFromPS = model.getNeighbors(nodes)
          println(s"super, pull ${nodes.length} neighbors from ps, cost ${System.currentTimeMillis() - before} ms.")
          nodes.flatMap { src =>
            val neighbors = neisFromPS.get(src)
            val (nbrs, tags, attrs) = (neighbors.getNeighborIds, neighbors.getTags, neighbors.getAttrs)
            nbrs.indices.filter(nbrs(_) > src).map(x => (src, nbrs(x), tags(x), attrs(x)))
          }.toIterator
        } else Iterator.empty
      }.repartition(partitionNum).persist(storageLevel)
      println(s"edges' partitionNum: ${edges.getNumPartitions}")
      println(s"num superEdges: ${edges.count()} ")

      edges.mapPartitionsWithIndex { case (index, iter) =>
        var startTs = System.currentTimeMillis()
        var computeStartTs = System.currentTimeMillis()
        if (iter.nonEmpty) {
          iter.sliding(batchSize, batchSize).flatMap { batchIter =>
            val endTs = System.currentTimeMillis()
            println(s"superPart $index, last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
            startTs = System.currentTimeMillis()
            val data = batchIter.toArray
            val nodes2pull = data.flatMap(x => Iterator(x._1, x._2)).distinct
            val before = System.currentTimeMillis()
            val nbrs = model.getNeighbors(nodes2pull)
            println(s"superPart $index, process ${batchIter.length} edges, pull ${nodes2pull.length} neighbors from ps, " +
              s"cost ${System.currentTimeMillis() - before} ms.")
            computeStartTs = System.currentTimeMillis()
            data.flatMap { case (src, dst, tag, attr) =>
              val srcNbrs = nbrs.get(src)
              val dstNbrs = nbrs.get(dst)
              val commonNbrs = NodeNeighborPartition.intersect(srcNbrs, dstNbrs).toMap
              NodeNeighborPartition.computeMotifCountV2(src, dst, (tag, attr), srcNbrs, dstNbrs, commonNbrs, isWeighted)
            }.toIterator
          }
        } else Iterator.empty
      }
    } else {
      val sc = SparkContext.getOrCreate()
      val motifCount = new ArrayBuffer[((Long, Byte), Float)]()
      sc.parallelize(motifCount, 1)
    }
  }

}
