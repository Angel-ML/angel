package com.tencent.angel.spark.ml.graph.algo.rank.kcore

import com.tencent.angel.model.output.format.SnapshotFormat
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import com.tencent.angel.spark.ml.graph.utils.params._
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class KCoreCompress(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasBalancePartitionPercent with HasPullBatchSize with HasTopK with HasBatchSize {

  def this() = this(Identifiable.randomUID("KCoreCompress"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)

    edges.persist(StorageLevel.DISK_ONLY)

    val neighborTable = KCoreCompress.edges2NeighborTable(edges, $(partitionNum), $(topK)).persist(StorageLevel.DISK_ONLY)
    val stats = KCoreCompress.statistics(neighborTable)
    println(s"minId=${stats._1} maxId=${stats._2} numNodes=${stats._3} numEdges=${stats._4}")
    val nodes = neighborTable.map(_._1)

    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val (tagModel, tagPsMatrix) = TagPSModel.fromMinMax(stats._1, stats._2 + 1, nodes, $(psPartitionNum))
    tagModel.initMsgs(nodes, $(batchSize))
    //tagPsMatrix.checkpoint()

    val neatNeighborTable = neighborTable.mapPartitionsWithIndex { case (index, iter) =>
      KCoreCompress.filterEdges(index, iter, tagModel, $(pullBatchSize), $(topK))
    }.persist($(storageLevel))
    neighborTable.unpersist()
    val statsNeat = KCoreCompress.statistics(neatNeighborTable)
    println(s"neat: minId=${statsNeat._1} maxId=${statsNeat._2} numNodes=${statsNeat._3} " +
      s"numEdges=${statsNeat._4}")

    val (minId, maxId, numEdges) = edges.mapPartitions(summarizeApplyOp)
      .reduce(summarizeReduceOp)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    val index = edges.flatMap(f => Iterator(f._1, f._2))
    //    val maxId = index.max() + 1
    //    val minId = index.min()
    //    val numEdges = edges.count()

    //    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val (model, psMatrix) = KCoreCompressModel.fromMinMax(minId, maxId + 1, index, $(psPartitionNum),
      $(useBalancePartition), $(balancePartitionPercent))
    var graph = neatNeighborTable.mapPartitionsWithIndex((index, it) =>
        Iterator(KCoreCompressPartition.apply1(index, it)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.foreach(_._1.initMsgs(model, $(batchSize)))
    val matrixNames = new Array[String](2)
    matrixNames(0) = tagPsMatrix.name
    matrixNames(1) = psMatrix.name
    //psMatrix.checkpoint()
    checkpoint(0, matrixNames)


    var curIteration = 0
    var numMsgs = model.numMsgs()
    var prev = graph
    println(s"numMsgs=$numMsgs")

    var preMinCore = 0
    var curMinCore = 0
    var changedMinCore = false
    var numLeftEdges = 0d
    do {
      curIteration += 1
      if (changedMinCore) {
        changedMinCore = false
        graph = prev.map(_._1.compress(model, numMsgs, preMinCore, $(topK)))
        graph.persist($(storageLevel))
        graph.count()
        prev.unpersist()
        numLeftEdges = graph.map(_._1.numEdges()).sum()
        prev = graph
        preMinCore = curMinCore
        println(s"compress: curIteration=$curIteration preMinCore=$preMinCore curMinCore=$curMinCore, numLeftEdges: $numLeftEdges")
      } else {
        graph = prev.map(_._1.processByBatch(model, numMsgs, curIteration, $(pullBatchSize)))
        graph.persist($(storageLevel))
        graph.count()
        prev.unpersist()
        curMinCore = graph.map(x => x._2).min()
        if (curIteration == 1) preMinCore = curMinCore
        if (curMinCore > preMinCore || curMinCore <= ${topK}) {
          changedMinCore = true
        }
        prev = graph
        model.resetMsgs()
        numMsgs = model.numMsgs()
        println(s"process : curIteration=$curIteration numMsgs=$numMsgs preMinCore = $preMinCore curMinCore = $curMinCore")
      }

    } while (numMsgs > 0)

    val retRDD = graph.map(_._1).map(_.save()).flatMap(f => f._1.zip(f._2)).filter(_._2 > $(topK)).sortByKey()
      .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCoreIdCol)}", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def summarizeApplyOp(iterator: Iterator[(Long, Long)]): Iterator[(Long, Long, Long)] = {
    var minId = Long.MaxValue
    var maxId = Long.MinValue
    var numEdges = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (src, dst) = (entry._1, entry._2)
      minId = math.min(minId, src)
      minId = math.min(minId, dst)
      maxId = math.max(maxId, src)
      maxId = math.max(maxId, dst)
      numEdges += 1
    }

    Iterator.single((minId, maxId, numEdges))
  }

  def summarizeReduceOp(t1: (Long, Long, Long),
                        t2: (Long, Long, Long)): (Long, Long, Long) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2), t1._3 + t2._3)

  def checkpoint(checkpointId: Int, names: Array[String]) = {
    val saveContext = new ModelSaveContext()
    names.foreach(matrix => {
      saveContext.addMatrix(new MatrixSaveContext(matrix, classOf[SnapshotFormat].getTypeName))
    })

    PSContext.instance().checkpoint(checkpointId, saveContext)
  }

}

object KCoreCompress {

  def statistics(neighborTable: RDD[(Long, Array[Long])]): (Long, Long, Long, Long) = {
    neighborTable.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      var numNodes = 0L
      iter.foreach { case(src, neighbors) =>
        min = math.min(min, src)
        max = math.max(max, src)
        numNodes += 1
        numEdges += neighbors.length
      }
      Iterator.single(( min, max, numNodes, numEdges))
    }.reduce{ case (c1, c2) =>
      (c1._1 min c2._1, c1._2 max c2._2, c1._3 + c2._3, c1._4 + c2._4)
    }
  }

  def edges2NeighborTable(edges: RDD[(Long, Long)], partitionNum: Int, degreeThreshold: Int): RDD[(Long, Array[Long])] = {
    edges.flatMap(x => Iterator((x._1, x._2), (x._2, x._1))).groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          val neis = group.toArray.distinct
          if (neis.length > degreeThreshold) Iterator.single(src, neis)
          else Iterator.empty
        }
      } else Iterator.empty
    }
  }

  def filterEdges(index: Int, iter: Iterator[(Long, Array[(Long)])], tagModel: TagPSModel,
                  batchSize: Int, degreeThreshold: Int): Iterator[(Long, Array[Long])] = {
    BatchIter(iter, batchSize).flatMap { batchIter =>
      val pullNodes = new mutable.HashSet[Long]()
      val localNeighborTable = new Long2ObjectOpenHashMap[Array[Long]](batchSize)
      batchIter.foreach { case (src, neighbors) =>
        localNeighborTable.put(src, neighbors)
        if (localNeighborTable.containsKey(src))
          pullNodes ++= neighbors
      }
      val beforePullTs = System.currentTimeMillis()
      val tags = tagModel.readMsgs(pullNodes.toArray)
      println(s"partition $index: process ${batchIter.length} nodes, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      val srcNodes = localNeighborTable.keySet().toLongArray
      srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src)
        val valid = srcNeighbors.filter(x => tags.get(x) == 1)
        if (valid.length > degreeThreshold) Iterator.single(src, valid) else Iterator.empty
      }
    }
  }
}
