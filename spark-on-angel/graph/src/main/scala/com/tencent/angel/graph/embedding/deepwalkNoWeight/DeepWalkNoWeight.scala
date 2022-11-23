package com.tencent.angel.graph.embedding.deepwalkNoWeight

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.graph.utils.{GraphIO, Stats}

class DeepWalkNoWeight(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasMaxIteration
  with HasBatchSize with HasArrayBoundsPath  with HasUseBalancePartition with HasDynamicInitNeighbor
  with HasNeedReplicaEdge with HasUseEdgeBalancePartition with HasWalkLength with HasEpochNum with HasBalancePartitionPercent {

  private var output: String = _

  def this() = this(Identifiable.randomUID("DeepWalk"))

  def setOutputDir(in: String): Unit = {
    output = in
  }

  def initialize(dataset: Dataset[_]): (DeepWalkPSModel, RDD[DeepWalkGraphPartition]) = {
    println("start deepwalk with no weight ... ")
    //create origin edges RDD and data preprocessing
    val rawEdges_ = NeighborDataOps.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol),  $(needReplicaEdge), true)
    val rawEdges = rawEdges_.repartition($(partitionNum)).persist(StorageLevel.DISK_ONLY)

    val (minId, maxId, numEdges) = Stats.summarize(rawEdges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    //ps process;create ps nodes adjacency matrix
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create model
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "deepwalk", SparkContext.getOrCreate().hadoopConfiguration)

    val data = rawEdges.flatMap(f => Iterator(f._1, f._2))
    val model = DeepWalkPSModel(modelContext, data, $(useBalancePartition), $(balancePartitionPercent), $(dynamicInitNeighbor))

    // calc neighbor table for each node
    val start = System.currentTimeMillis()

    val graphOri = if ($(dynamicInitNeighbor)) {
      println("Update the adjacency list using dynamic initialization")
      // ps loadBalance by in degree
      val degree = rawEdges.map(x => (x._1, 1)).reduceByKey(_ + _)
      degree.persist($(storageLevel))
      val numNodes = degree.count()
      println(s"num src nodes: $numNodes")

      var startTime = System.currentTimeMillis()
      val initNumNodes = model.initNeighborsDegree(degree, $(batchSize))
      println(s"init $initNumNodes nodes' space to PS, cost ${System.currentTimeMillis() - startTime} ms.")

      startTime = System.currentTimeMillis()
      val numEdges = model.addNodeNeiWithNoWeight(rawEdges, $(batchSize))
      println(s"add $numEdges edges to PS, cost ${System.currentTimeMillis() - startTime} ms.")

      val graphOri = degree.mapPartitionsWithIndex((index, edges) =>
        Iterator(DeepWalkGraphPartition.initNodePaths(index, edges.map(r => r._1).toArray, $(batchSize))))
      graphOri
    } else {
      println("Update the adjacency list using static initialization")
      val neighborTable = rawEdges.groupByKey($(partitionNum)).map(x => (x._1, x._2.toArray.distinct))

      //push node adjacency list into ps matrix;create graph with （node，sample path）
      println("start initPSMatrixAndNodePath")
      var startTime = System.currentTimeMillis()
      val numEdges = neighborTable.mapPartitionsWithIndex((index, adjTable) =>
        DeepWalkGraphPartition.initPSMatrix(model, index, adjTable, $(batchSize))).reduce(_ + _)
      println(s"init $numEdges edges to PS, cost ${System.currentTimeMillis() - startTime} ms.")

      val graphOri = neighborTable.mapPartitionsWithIndex((index, adjTable) =>
        Iterator(DeepWalkGraphPartition.initNodePaths(index, adjTable.map(r => r._1).toArray, $(batchSize))))
      graphOri
    }

    graphOri.persist($(storageLevel))
    //trigger action
    graphOri.foreachPartition(_ => Unit)

    println(s"initialize neighborTable cost ${(System.currentTimeMillis() - start) / 1000}s")
    // checkpoint
    model.checkpoint()
    (model, graphOri)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val (model, graphOri) = initialize(dataset)

    println("start transform")
    var epoch = 0
    while (epoch < $(epochNum)) {
      var graph = graphOri.map(x => x.deepClone())
      //sample paths with random walk
      var curIteration = 0
      var prev = graph
      val beginTime = System.currentTimeMillis()
      do {
        val beforeSample = System.currentTimeMillis()
        curIteration += 1
        graph = prev.map(_.process(model, curIteration, $(dynamicInitNeighbor)))
        graph.persist($(storageLevel))
        graph.count()
        prev.unpersist(true)
        prev = graph
        var sampleTime = (System.currentTimeMillis() - beforeSample)
        println(s"epoch $epoch, iter $curIteration, sampleTime: $sampleTime")
      } while (curIteration < $(walkLength) - 1)


      val EndTime = (System.currentTimeMillis() - beginTime)
      println(s"epoch $epoch, DeepWalkWithWeight all sampleTime: $EndTime")

      val temp = graph.flatMap(_.save())
      println(s"epoch $epoch, num path: ${temp.count()}")
      println(s"epoch $epoch, num invalid path: ${
        temp.filter(_.length != ${
          walkLength
        }).count()
      }")
      val tempRe = dataset.sparkSession.createDataFrame(temp.map(x => Row(x.mkString(" "))), transformSchema(dataset.schema))
      if (epoch == 0) {
        GraphIO.save(tempRe, output)
      }
      else {
        GraphIO.appendSave(tempRe, output)
      }

      println(s"epoch $epoch, saved results to $output")
      epoch += 1
      graph.unpersist()
    }

    val t = SparkContext.getOrCreate().parallelize(List("1", "2"), 1)
    dataset.sparkSession.createDataFrame(t.map(x => Row(x)), transformSchema(dataset.schema))
  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("path", StringType, nullable = false)))
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

