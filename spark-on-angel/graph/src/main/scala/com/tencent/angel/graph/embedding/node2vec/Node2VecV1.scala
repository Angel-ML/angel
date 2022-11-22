package com.tencent.angel.graph.embedding.node2vec

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborTableModel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.graph.utils.params.HasDynamicInitNeighbor
import com.tencent.angel.graph.utils.{GraphIO, Stats}

import scala.util.Random

class Node2VecV1(override val uid: String) extends Node2Vec(uid) with HasDynamicInitNeighbor {
  def this() {
    this(s"Node2VecV1_${(new Random).nextInt()}")
  }

  private def createWalkPathAndPushNeighbor(neighborTableName: String, dataset: Dataset[_],
                                            srcNodeIdCol: String,
                                            dstNodeIdCol: String,
                                            needEdgeReplica: Boolean): RDD[Long] = {

    val rawEdges = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
      .repartition($(partitionNum))
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)

    rawEdges.persist(StorageLevel.DISK_ONLY)

    val (minId, maxId, numEdges_) = rawEdges.mapPartitions(Stats.summarizeApplyOp).reduce(Stats.summarizeReduceOp)

    val degree = if (needEdgeReplica) {
      rawEdges.flatMap(x => Iterator((x._1, 1), (x._2, 1))).reduceByKey(_ + _)
    } else
      rawEdges.map(x => (x._1, 1)).reduceByKey(_ + _)
    degree.persist($(storageLevel))
    val (minDegree,maxDegree,numNodes) = Stats.summarizeForDegree(degree)

    println(s"min node id = ${minId}")
    println(s"max node id = ${maxId}")
    println(s"num of nodes = ${numNodes}")
    println(s"num of edges = ${numEdges_}")
    println(s"minDegree = ${minDegree}")
    println(s"maxDegree = ${maxDegree}")

    // Start PS and init the model, push aliasTable to ps
    println("start to run ps")
    Node2Vec.startPS(dataset.sparkSession.sparkContext)

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, numNodes,
      "simple_neighbor", SparkContext.getOrCreate().hadoopConfiguration)
    val model = new DynamicSimpleNeighborTableModel(modelContext)
    model.init()
    neighbor = model.neighborMatrix

    val initNumNodes = model.initNeighbors(degree, $(batchSize))
    println(s"init $initNumNodes nodes' space.")

    val numEdges = model.addNeighbors(rawEdges, $(batchSize), needEdgeReplica)
    println(s"add $numEdges edges.")

    degree.map(_._1)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    conf = dataset.sparkSession.sparkContext.getConf

    // create neighborTable and distribute it on PSs
    println("begin to push NeighborTable ...")
    val start = System.currentTimeMillis()
    val nodes = createWalkPathAndPushNeighbor("NeighborTable", dataset,
      $(srcNodeIdCol), $(dstNodeIdCol), $(needReplicaEdge))
    val end = System.currentTimeMillis()
    println(s"finish pushing NeighborTable, time: ${(end - start) / 1000.0}s")

    // sampling
    var epoch = 0
    while (epoch < $(epochNum)) {
      // create and initialize walkpath matrix on executors
      val start = System.currentTimeMillis()
      println(s"start initializing the walkPath...")
      var curIteration = 1
      val iniWalkPath = nodes.mapPartitionsWithIndex((index, nodePartition) =>
        Iterator(Node2VecGraphPartition.initNodePaths(index, neighbor, nodePartition, $(batchSize), $(pValue), $(qValue),
          curIteration, $(isTrunc), $(truncLength), $(dynamicInitNeighbor))))
      iniWalkPath.persist($(storageLevel))
      //trigger action
      iniWalkPath.foreachPartition(_ => Unit)
      nodes.unpersist()
      println(s"finish initializing walkPath, and each initial path contains 2 nodes. Initializing Time: " +
        s"${(System.currentTimeMillis() - start) / 1000}s.")
      var graph = iniWalkPath.map(x => x.pathsClone())
      graph.persist($(storageLevel))
      graph.count()
      //sample paths with random walk
      var prev = graph
      val beginTime = System.currentTimeMillis()
      do {
        val beforeSample = System.currentTimeMillis()
        curIteration += 1
        graph = prev.map(_.process(neighbor, curIteration, $(dynamicInitNeighbor)))
        graph.persist($(storageLevel))
        graph.count()
        prev.unpersist()
        prev = graph
        var sampleTime = (System.currentTimeMillis() - beforeSample)
        println(s"epoch $epoch, iter $curIteration, sampleTime: ${sampleTime / 1000}s")
      } while (curIteration < $(walkLength) - 1)
      iniWalkPath.unpersist()

      val EndTime = (System.currentTimeMillis() - beginTime)
      println(s"epoch $epoch, total sampleTime: ${EndTime / 1000}s")

      val temp = graph.flatMap(_.save())
      temp.persist(StorageLevel.DISK_ONLY)
      println(s"epoch $epoch, num path: ${temp.count()}")
      val tempRe = dataset.sparkSession.createDataFrame(temp.map(x => Row(x.mkString(" "))), transformSchema(dataset.schema))
      if (epoch == 0) {
        GraphIO.save(tempRe, output)
      } else {
        GraphIO.appendSave(tempRe, output)
      }
      println(s"epoch $epoch, saved results to $output")
      epoch += 1
      temp.unpersist()
      graph.unpersist()

    }

    val t = SparkContext.getOrCreate().parallelize(List("1", "2"), 1)
    dataset.sparkSession.createDataFrame(t.map(x => Row(x)), transformSchema(dataset.schema))
  }
}
