package com.tencent.angel.spark.ml.graph.algo.statistics.triangle

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.data.neighbor.{NeighborTable, NodeNeighborPartition, NodeNeighborsOps}
import com.tencent.angel.spark.ml.graph.utils.{GraphIO, Stats}
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

class WeightedMotifCountingV2(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize with HasWeightCol
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasInput with HasExtraInputs
  with HasDelimiter with HasOutputTriangleCol with HasIsWeighted {

  def this() = this(Identifiable.randomUID("motif_counting_directed_v2"))

  var userGroup: RDD[(Long, Long)] = _
  def setUserGroup(in: RDD[(Long, Long)]): Unit = { this.userGroup = in }

  var hasEdgeType: Boolean = false
  def setHasEdgeType(in: Boolean): Unit = { this.hasEdgeType = in }

  var outputDir: String = _
  def setOutputDir(in: String): Unit = { this.outputDir = in }

  override def transform(dataset: Dataset[_]): DataFrame = {
    // get stats of all edges
    val (minId, maxId, numEdges) = WeightedMotifCountingV2.getStats(dataset)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    // Start up Angel ps firstly
    println(s"start parameter server")
    val psStartTime = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"create ps model")
    val modelContext = new ModelContext(
      $(psPartitionNum), minId, maxId+1, -1, "neighbor", SparkContext.getOrCreate().hadoopConfiguration)
    val tagContext = if (userGroup != null) {
      new ModelContext($(psPartitionNum),  minId, maxId+1, -1, "tag", SparkContext.getOrCreate().hadoopConfiguration)
    } else null
    val model = new MotifNeighborModel(modelContext, tagContext)
    model.init()

    // init tag if there exists userGroup rdd
    val beforeInitTag = System.currentTimeMillis()
    if (userGroup != null) {
      userGroup.groupByKey($(partitionNum))
        .map(x => (x._1, x._2.toArray.sorted))
        .foreachPartition { iter => model.updateTag(iter, $(batchSize)) }
      println(s"push tag cost ${System.currentTimeMillis() - beforeInitTag} ms")
    }

    // calc by edge types
    val edgeTypes: Array[Long] = if (hasEdgeType) WeightedMotifCountingV2.getEdgeTypes(dataset) else Array()
    if (edgeTypes.isEmpty) {
      val edges = WeightedMotifCountingV2.loadEdges(dataset, $(isWeighted))
      val motif = calcMotif(edges, model)
      val output = motif.map { case (nodeId, tc) =>
        Row(nodeId, tc(0), tc(1), tc(2), tc(3), tc(4), tc(5), tc(6), tc(7), tc(8), tc(9), tc(10),
          tc(11), tc(12), tc(13), tc(14), tc(15), tc(16), tc(17), tc(18), tc(19), tc(20), tc(21),
          tc(22), tc(23), tc(24), tc(25), tc(26), tc(27), tc(28), tc(29), tc(30), tc(31), tc(32))
      }
      val outputSchema = transformSchema(dataset.schema)
      val df = dataset.sparkSession.createDataFrame(output, outputSchema)
      GraphIO.save(df, outputDir)
    } else {
      for (index <- edgeTypes.indices) {
        println(s"num edgeTypes=${edgeTypes.length}")
        println(s"------ edge type ${edgeTypes(index)} ------")
        val edges = WeightedMotifCountingV2.loadEdges(dataset, $(isWeighted), edgeTypes(index))
        val motif = calcMotif(edges, model)
        // output contains an extra line of edge type
        val output = motif.map { case (nodeId, tc) =>
          Row(nodeId, edgeTypes(index), tc(0), tc(1), tc(2), tc(3), tc(4), tc(5), tc(6), tc(7), tc(8), tc(9), tc(10),
            tc(11), tc(12), tc(13), tc(14), tc(15), tc(16), tc(17), tc(18), tc(19), tc(20), tc(21),
            tc(22), tc(23), tc(24), tc(25), tc(26), tc(27), tc(28), tc(29), tc(30), tc(31), tc(32))
        }
        val outputSchema = transformSchema(dataset.schema)
        val df = dataset.sparkSession.createDataFrame(output, outputSchema)
        if (index == 0) GraphIO.save(df, outputDir) else GraphIO.appendSave(df, outputDir)
        // clear neighbors on ps, prepare for the next neighbor initiation
        model.clearNeighbors()
      }
    }

    // return
    val t = SparkContext.getOrCreate().parallelize(List("1", "2"), 1)
    val schema = StructType(Seq(StructField("temp", StringType, nullable = false)))
    dataset.sparkSession.createDataFrame(t.map(x => Row(x)), schema)
  }

  override def transformSchema(schema: StructType): StructType = {
    val seq = new ArrayBuffer[StructField]()
    seq.append(StructField($(srcNodeIdCol), LongType, nullable = false))
    if (hasEdgeType) seq.append(StructField("edgeType", LongType, nullable = false))
    for (i <- 1 to 33) {
      seq.append(StructField("motif-"+i, FloatType, nullable = false))
    }
    StructType(seq)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def calcMotif(edges: RDD[(Long, Long, Float)], model: MotifNeighborModel) = {
    // Build neighbor rdd and worker neighbor table
    println(s"build neighbor table partition")
    val neighborRDDForWorkerPS = WeightedMotifCountingV2.neighborAttr(edges, $(partitionNum))
    val neighborPartitions = NodeNeighborPartition.fromNeighborTableRDD(neighborRDDForWorkerPS)
    neighborPartitions.persist($(storageLevel))
    // Get statics for sub-graph in task partition
    val stats = NodeNeighborPartition.getStats[Float](neighborPartitions)
    println(s"calculate stats of graph, count the number of vertices and edges " + stats)
    // Init graph on PS
    val initTableStartTime = System.currentTimeMillis()
    val neighborDataOps = new NodeNeighborsOps(model)
    neighborDataOps.initLongNeighborAttrTag(neighborPartitions, $(batchSize))
    println(s"push costs ${System.currentTimeMillis() - initTableStartTime} ms")

    // Write checkpoint for model on PS
    val cpTableStartTime = System.currentTimeMillis()
    println(s"checkpoint neighbor table")
    model.checkpoint()
    println(s"checkpoint costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    // Calculate triangle count
    println(s"calculate motif")
    val resRdd = if (userGroup == null)
      neighborDataOps.calWeightedMotifV2(neighborPartitions, $(pullBatchSize), $(isWeighted)).persist($(storageLevel))
    else neighborDataOps.calWeightedGroupMotifV2(neighborPartitions, $(pullBatchSize), $(isWeighted)).persist($(storageLevel))

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
    motif
  }
}


object WeightedMotifCountingV2 {

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

  def getStats(dataset: Dataset[_]): (Long, Long, Long) = {
    val edges = dataset.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)
    Stats.summarize(edges)
  }

  def loadEdges(dataset: Dataset[_], isWeighted: Boolean): RDD[(Long, Long, Float)] = {
    if (isWeighted) {
      dataset.select("src", "dst", "weight").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2 && f._3 != 0.0f)
    } else {
      dataset.select("src", "dst").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(f => f._1 != f._2)
    }
  }

  def loadEdges(dataset: Dataset[_], isWeighted: Boolean, edgeType: Long): RDD[(Long, Long, Float)] = {
    if (isWeighted) {
      dataset.select("src", "dst", "edgeType", "weight").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getLong(2), row.getFloat(3)))
        .filter(f => f._3 == edgeType && f._1 != f._2 && f._4 != 0.0f)
        .map(f => (f._1, f._2, f._4))
    } else {
      dataset.select("src", "dst", "edgeType").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getLong(2), 1.0f))
        .filter(f => f._3 == edgeType && f._1 != f._2)
        .map(f => (f._1, f._2, f._4))
    }
  }

  def getEdgeTypes(dataset: Dataset[_]): Array[Long] = {
    dataset.select("edgeType").rdd
      .map(x => (x.getLong(0), 1))
      .reduceByKey(_ + _)
      .map(_._1)
      .collect()
  }

}
