package com.tencent.angel.spark.ml.graph.algo.communitydetection.subgraph

import com.tencent.angel.spark.context.PSContext
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


class SeedNeighbors(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasNodeAttrPath with HasMaxIteration {


  def this() = this(Identifiable.randomUID("SeedNeighbors"))

  var seedNodes: RDD[Long] = _
  var output: String = _
  var percentageThreshold: Float = 100f
  var maxNodeDegree: Int = 5000000

  def setNodeAttr(nodes: RDD[Long]): Unit = {
    this.seedNodes = nodes
  }

  def setOutput(str: String): Unit = {
    this.output = str
  }

  def setMaxNodeDegree(d: Int): Unit = { this.maxNodeDegree = d }
  def setPercentageThreshold(p: Float): Unit = { this.percentageThreshold = p }

  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    //edges's storageLevel choose  Disk_Only
    edges.persist(StorageLevel.DISK_ONLY)

    println(s"sample edges:")
    println(edges.take(10).mkString(","))

    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val (minId, maxId, numEdges) = edges.mapPartitions(Stats.summarizeApplyOp)
      .reduce(Stats.summarizeReduceOp)

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    //     Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = SeedPSModel.fromMinMax($(batchSize), minId, maxId + 1, nodes, $(psPartitionNum), $(useBalancePartition))

    val numSeeds = seedNodes.count()
    println(s"numSeeds=$numSeeds")

    val graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(SeedGraphPartition.apply(index, adjTable)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    var numMsgs = model.initAttributes(this.seedNodes, $ {batchSize})
    model.checkpoint()

    // mark neg nodes on ps
    val negNodes = getNegNodes(maxNodeDegree, percentageThreshold, graph)
    if (negNodes.nonEmpty) {
      model.initNegNodes(negNodes)
      model.checkpoint(1)
    }

    var curIteration = 0
    do {
      curIteration += 1
      val re = graph.map(_.process(model, numMsgs, curIteration))
      re.count()
      model.checkpoint(curIteration+1)
      numMsgs = model.numMsgs()
      println(s"curIteration=$curIteration numMsgs=$numMsgs")
    } while (curIteration < 2)

    val resRDD = graph.map(_.getRes(model)).persist(StorageLevel.DISK_ONLY)
    val resNodes = resRDD.flatMap(_._1).map(x => (x._1, x._2 - 1)).map(r => Row.fromSeq(Seq[Any](r._1, r._2)))
    val resEdges = resRDD.flatMap(_._2).map(r => Row.fromSeq(Seq[Any](r._1, r._2)))

    val nodesDF = dataset.sparkSession.createDataFrame(resNodes, transformSchema(dataset.schema))
    val edgesDF = dataset.sparkSession.createDataFrame(resEdges,transformSchema2(dataset.schema))
    GraphIO.save(edgesDF, output + "/edges")
    GraphIO.save(nodesDF, output + "/nodes")

    nodesDF
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"node", LongType, nullable = false),
      StructField(s"distance", IntegerType, nullable = false)
    ))
  }

  def transformSchema2(schema:StructType):StructType = {
    StructType(Seq(
      StructField(s"src", LongType, nullable = false),
      StructField(s"dst", LongType, nullable = false)
    ))
  }

  def getNegNodes(maxNodeDegree: Int, percentageThreshold: Float, graph: RDD[SeedGraphPartition]): Array[Long] = {
    val degree = graph.flatMap(_.degree()).persist(StorageLevel.MEMORY_ONLY)
    val discardKeys = if (maxNodeDegree > 0) {
      degree.filter(_._2 > maxNodeDegree).collect().sortWith(_._2 > _._2)
    } else {
      val numKeys = degree.count()
      val discardNum = (numKeys * (1 - percentageThreshold)).toInt
      degree.sortBy(_._2, false).take(discardNum)
    }
    degree.unpersist()
    if (discardKeys.nonEmpty) {
      println(s"discard ${discardKeys.length} nodes, discarded maxDegree=${discardKeys.head}, " +
        s"discarded minDegree=${discardKeys.last}.")
      discardKeys.map(_._1)
    } else {
      println(s"no keys should be discarded.")
      new Array[Long](0)
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
