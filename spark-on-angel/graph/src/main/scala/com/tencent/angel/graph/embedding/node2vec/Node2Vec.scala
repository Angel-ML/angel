package com.tencent.angel.graph.embedding.node2vec

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.LongKeysUpdateParam
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.model.neighbor.simple.psf.init.InitNeighbors
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.{IElement, LongArrayElement}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl
import com.tencent.angel.graph.embedding.deepwalk.DeepWalk
import com.tencent.angel.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class Node2Vec(override val uid: String) extends Transformer
  with Node2VecParams with DefaultParamsWritable with Logging {
  var neighbor: PSMatrix = _
  var minId: Long = -1
  var maxId: Long = -1
  var index: RDD[Long] = _
  var conf: SparkConf = _
  var output: String = _

  def this() {
    this(s"Node2Vec_${(new Random).nextInt()}")
  }

  def setOutputDir(in: String): Unit = {
    output = in
  }

  private def createMatrix(name: String, numRow: Int, minId: Long, maxId: Long, rowType: RowType,
                           psNumPartition: Int, data: RDD[(Long, Long)],
                           useBalancePartition: Boolean, percent: Float): PSMatrixImpl = {

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId, -1, name,
      SparkContext.getOrCreate().hadoopConfiguration)
    val matrix = ModelContextUtils.createMatrixContext(modelContext, rowType,
      classOf[LongArrayElement])

    if (useBalancePartition && (!modelContext.isUseHashPartition)) {
      index = data.flatMap(f => Iterator(f._1, f._2))
        .persist($(storageLevel))
      LoadBalancePartitioner.partition(index, modelContext.getMaxNodeId,
        modelContext.getPartitionNum, matrix, percent)
    }

    val psMatrix = PSMatrix.matrix(matrix)
    val psMatrixImpl = new PSMatrixImpl(psMatrix.id, matrix.getName, 1,
      modelContext.getMaxNodeId, matrix.getRowType)

    if (useBalancePartition && (!modelContext.isUseHashPartition))
      index.unpersist()

    psMatrixImpl
  }

  private def createWeightedMatrix(name: String, numRow: Int, minId: Long, maxId: Long,
                                   rowType: RowType, psNumPartition: Int,
                                   data: RDD[(Long, Long, Float)],
                                   useBalancePartition: Boolean, percent: Float): PSMatrix = {

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId, -1, name,
      SparkContext.getOrCreate().hadoopConfiguration)
    val matrix = ModelContextUtils.createMatrixContext(modelContext, rowType, classOf[AliasElement])

    if (useBalancePartition && (!modelContext.isUseHashPartition)) {
      index = data.flatMap(f => Iterator(f._1, f._2))
        .persist($(storageLevel))
      LoadBalancePartitioner.partition(index, modelContext.getMaxNodeId,
        modelContext.getPartitionNum, matrix, percent)
    }

    val psMatrix = PSMatrix.matrix(matrix)

    if (useBalancePartition && (!modelContext.isUseHashPartition))
      index.unpersist()

    psMatrix
  }

  private def initNeighbors(neighborTable: Seq[(Long, Array[Long])]): Unit = {
    val nodeIds = new Array[Long](neighborTable.size)
    val neighborElems = new Array[IElement](neighborTable.size)
    neighborTable.zipWithIndex.foreach(e => {
      nodeIds(e._2) = e._1._1
      neighborElems(e._2) = new LongArrayElement(e._1._2)
    })

    neighbor.psfUpdate(new InitNeighbors(new LongKeysUpdateParam(neighbor.id, nodeIds,
      neighborElems))).get()
  }

  private def initAliasTable(neighborTable: Seq[(Long, Array[Long], Array[Float], Array[Int])]):
  Unit = {
    val nodeIds = new Array[Long](neighborTable.size)
    val neighborElems = new Array[IElement](neighborTable.size)
    neighborTable.zipWithIndex.foreach(e => {
      nodeIds(e._2) = e._1._1
      neighborElems(e._2) = new AliasElement(e._1._2, e._1._3, e._1._4)
    })

    neighbor.psfUpdate(new InitNeighbors(new LongKeysUpdateParam(neighbor.id,
      nodeIds, neighborElems))).get()
  }

  private def createWalkPathAndPushNeighbor(neighborTableName: String, dataset: Dataset[_],
                                            srcNodeIdCol: String,
                                            dstNodeIdCol: String,
                                            needEdgeReplica: Boolean): RDD[(Long, Array[Long])] = {

    dataset.persist($(storageLevel))

    val nonEmpty = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
      .repartition($(partitionNum))
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)

    // create replicated edges
    val replicaDataset = if (needEdgeReplica) {
      nonEmpty.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
    } else {
      nonEmpty.map(f => (f._1, f._2))
    }
    // collect neighbors by keys
    val dataSetGroupByKey = replicaDataset.groupByKey($(partitionNum))
      .map(e => (e._1, e._2.toArray))
    dataSetGroupByKey.persist($(storageLevel))
    val stats = NeighborDataOps.statsByNeighborTable(dataSetGroupByKey)
    println(s"min node id = ${stats._1}")
    println(s"max node id = ${stats._2}")
    println(s"num of nodes = ${stats._3}")
    println(s"num of edges = ${stats._4}")
    println(s"minDegree = ${stats._5}")
    println(s"maxDegree = ${stats._6}")

    minId = stats._1
    maxId = stats._2 + 1

    println("Start PS ...")
    Node2Vec.startPS(dataset.sparkSession.sparkContext)
    println("PS Started!")
    val neighborMatrix = createMatrix(neighborTableName, 1, minId, maxId,
      RowType.T_ANY_LONGKEY_SPARSE, $(psPartitionNum),
      nonEmpty, ${useBalancePartition}, $(balancePartitionPercent))
    neighbor = neighborMatrix

    dataSetGroupByKey.mapPartitions { iter => {
      // Init the neighbor table use many mini-batch to avoid big object
      iter.sliding($(batchSize), $(batchSize)).map(pairs => initNeighbors(pairs))
    }
    }.count()
    dataset.unpersist()

    // checkpoint PSMatrix
    if ($(setCheckPoint)) {
      val start = System.currentTimeMillis()
      neighbor.checkpoint()
      println(s"the checkpoint time of neighbor PSMatrix is " +
        s"${(System.currentTimeMillis() - start) / 1000}s.")
    }

    dataSetGroupByKey
  }

  private def createAndPushAlias(neighborTableName: String, dataset: Dataset[_],
                                 srcNodeIdCol: String,
                                 dstNodeIdCol: String,
                                 weightCol: String,
                                 needEdgeReplica: Boolean):
  RDD[(Long, Array[Long], Array[Float], Array[Int])] = {
    dataset.persist($(storageLevel))

    val nonEmpty = dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd
      .repartition($(partitionNum))
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
      .filter(f => f._1 != f._2)

    // transform undirected node pairs to directed ones
    val replicaDataset = if (needEdgeReplica) {
      nonEmpty.flatMap(f => Iterator((f._1, (f._2, f._3)), (f._2, (f._1, f._3))))
    } else {
      nonEmpty.map(f => (f._1, (f._2, f._3)))
    }
    //calculate alias table
    val aliasTable = replicaDataset.groupByKey($(partitionNum)).map(x => (x._1, x._2.toArray.distinct))
      .mapPartitionsWithIndex { case (partId, iter) =>
        DeepWalk.calcAliasTable(partId, iter)
      }
    aliasTable.persist($(storageLevel))

    val aliasTableEdges = aliasTable.map(e => (e._1, e._2))
    val stats = NeighborDataOps.statsByNeighborTable(aliasTableEdges)
    println(s"min node id = ${stats._1}")
    println(s"max node id = ${stats._2}")
    println(s"num of nodes = ${stats._3}")
    println(s"num of edges = ${stats._4}")
    println(s"minDegree = ${stats._5}")
    println(s"maxDegree = ${stats._6}")

    minId = stats._1
    maxId = stats._2 + 1

    println("Start PS ...")
    Node2Vec.startPS(dataset.sparkSession.sparkContext)
    println("PS Started!")
    val neighborMatrix = createWeightedMatrix(neighborTableName, 1, minId, maxId, RowType.T_ANY_LONGKEY_SPARSE,
      $(psPartitionNum), nonEmpty, ${useBalancePartition},
      ${balancePartitionPercent})
    neighbor = neighborMatrix

    //update alias PSMatrix
    aliasTable.mapPartitions { iter => {
      // Init the alias table use many mini-batch to avoid big object
      iter.sliding($(batchSize), $(batchSize)).map(pairs => initAliasTable(pairs))
    }
    }.count()
    dataset.unpersist()

    // checkpoint PSMatrix
    val start = System.currentTimeMillis()
    neighbor.checkpoint()
    println(s"the checkpoint time of neighbor PSMatrix is ${(System.currentTimeMillis() - start) / 1000}s.")

    aliasTable
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    conf = dataset.sparkSession.sparkContext.getConf

    // create neighborTable and distribute it on PSs
    println("begin to push NeighborTable ...")
    val start = System.currentTimeMillis()
    val dataGroupByKey = createWalkPathAndPushNeighbor("NeighborTable", dataset,
      $(srcNodeIdCol), $(dstNodeIdCol), $(needReplicaEdge))
    val end = System.currentTimeMillis()
    println(s"finish pushing NeighborTable, time: ${(end - start) / 1000.0}s")

    // sampling
    var epoch = 0
    while (epoch < ${epochNum}) {
      // create and initialize walkpath matrix on executors
      val start = System.currentTimeMillis()
      println(s"start initializing the walkPath...")
      var curIteration = 1
      val iniWalkPath = dataGroupByKey.mapPartitionsWithIndex ( (index, neighborTab) =>
        Iterator( Node2VecGraphPartition.initNodePaths(index, neighborTab, $(batchSize), $(pValue), $(qValue),
          curIteration, $(useTrunc), $(truncLength))) )
      iniWalkPath.persist($(storageLevel))
      //trigger action
      iniWalkPath.foreachPartition(_ => Unit)
      dataGroupByKey.unpersist()
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
        graph = prev.map(_.process(neighbor, curIteration))
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

  def transformWithWeights(dataset: Dataset[_]): DataFrame = {
    conf = dataset.sparkSession.sparkContext.getConf

    // create neighborTable and distribute it on PSs
    println("begin to push NeighborTable ...")
    val start = System.currentTimeMillis()
    val aliasTable = createAndPushAlias("AliasTable", dataset,
      $(srcNodeIdCol), $(dstNodeIdCol), $(weightCol), $(needReplicaEdge))
    val end = System.currentTimeMillis()
    println(s"finish pushing NeighborTable, time: ${(end - start) / 1000.0}s")

    // sampling
    var epoch = 0
    while (epoch < ${epochNum}) {
      // create and initialize walkpath matrix on executors
      val start = System.currentTimeMillis()
      println(s"start initializing the walkPath...")
      var curIteration = 1
      val iniWalkPath = aliasTable.mapPartitionsWithIndex ( (index, neighborTab) =>
        Iterator( Node2VecGraphPartition.initNodePathsWithWeights(index, neighbor,
          neighborTab, $(batchSize), $(pValue), $(qValue), curIteration, $(useTrunc),
          $(truncLength))) )
      iniWalkPath.persist($(storageLevel))
      //trigger action
      iniWalkPath.foreachPartition(_ => Unit)
      aliasTable.unpersist()
      println("finish initializing walkPath, and each initial path contains 2 nodes. " +
        s"Initializing Time: ${(System.currentTimeMillis() - start) / 1000}s.")
      var graph = iniWalkPath.map(x => x.pathsClone())
      graph.persist($(storageLevel))
      graph.count()
      //sample paths with random walk
      var prev = graph
      val beginTime = System.currentTimeMillis()
      do {
        val beforeSample = System.currentTimeMillis()
        curIteration += 1
        graph = prev.map(_.processWithWeights(neighbor, curIteration))
        graph.persist($(storageLevel))
        graph.count()
        prev.unpersist()
        prev = graph
        val sampleTime = System.currentTimeMillis() - beforeSample
        println(s"epoch $epoch, iter $curIteration, sampleTime: ${sampleTime / 1000}s")
      } while (curIteration < $(walkLength) - 1)
      iniWalkPath.unpersist()

      val EndTime = (System.currentTimeMillis() - beginTime)
      println(s"epoch $epoch, total sampleTime: ${EndTime / 1000}s")

      val temp = graph.flatMap(_.save())
      temp.persist(StorageLevel.DISK_ONLY)
      println(s"epoch $epoch, num path: ${temp.count()}")
      val tempRe = dataset.sparkSession.createDataFrame(temp.map(x => Row(x.mkString(" "))),
        transformSchema(dataset.schema))
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



  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("path", StringType, nullable = false)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object Node2Vec extends DefaultParamsReadable[Node2Vec] {

  override def load(path: String): Node2Vec = super.load(path)

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  def stopPS(): Unit = {
    PSContext.stop()
  }

  def calcAliasTable(partId: Int, iter: Iterator[(Long, Array[(Long, Float)])]):
  Iterator[(Long, Array[Long], Array[Float], Array[Int])] = {
    iter.map { case (src, neighbors) =>
      val (events, weights) = neighbors.unzip
      val weightsSum = weights.sum
      val len = weights.length
      val areaRatio = weights.map(_ / weightsSum * len)
      val (accept, alias) = createAliasTable(areaRatio)
      (src, events, accept, alias)
    }
  }

  def createAliasTable(areaRatio: Array[Float]): (Array[Float], Array[Int]) = {
    val len = areaRatio.length
    val small = ArrayBuffer[Int]()
    val large = ArrayBuffer[Int]()
    val accept = Array.fill(len)(0f)
    val alias = Array.fill(len)(0)

    for (idx <- areaRatio.indices) {
      if (areaRatio(idx) < 1.0) small.append(idx) else large.append(idx)
    }
    while (small.nonEmpty && large.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      val largeIndex = large.remove(large.size - 1)
      accept(smallIndex) = areaRatio(smallIndex)
      alias(smallIndex) = largeIndex
      areaRatio(largeIndex) = areaRatio(largeIndex) - (1 - areaRatio(smallIndex))
      if (areaRatio(largeIndex) < 1.0) small.append(largeIndex) else large.append(largeIndex)
    }
    while (small.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      accept(smallIndex) = 1
    }

    while (large.nonEmpty) {
      val largeIndex = large.remove(large.size - 1)
      accept(largeIndex) = 1
    }
    (accept, alias)
  }

}
