package com.tencent.angel.graph.reindex

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.util.LogUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class ReIndexModel(dataset: Dataset[_], dataPartitionNum: Int, psPartitionNum: Int, srcNodeIdCol: String,
                   dstNodeIdCol: String, batchSize: Int) extends Serializable {
  /**
    * Model on PS
    */
  @volatile var psModel: ReIndexPSModel = _

  def action(mapsInput: RDD[(String, Long)]=null): (DataFrame, DataFrame) = {
    // Original edges
    val rawEdges: RDD[(String, String)] = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
      .map(row => (row.getString(0), row.getString(1)))
      .filter(f => !f._1.equals(f._2))
    rawEdges.persist(StorageLevel.DISK_ONLY)

    val nodes = rawEdges.flatMap(x => Seq(x._1, x._2)).map(e => (e, 1)).reduceByKey(_+_, dataPartitionNum).map(e => e._1)
    val rawMaps = if (mapsInput != null) {
      val mapsInputCount = mapsInput.count()
      val nodesInputRDD = mapsInput.map(f => f._1)
      val nodeSubtract = nodes.subtract(nodesInputRDD)
      println(s"previous mapping nodes num=$mapsInputCount, increasing nodes num=${nodeSubtract.count()}")

      val nodeSubtractWithIndex = nodeSubtract.zipWithIndex().map(ele => (ele._1, ele._2 + mapsInputCount))
      mapsInput.union(nodeSubtractWithIndex).persist(StorageLevel.DISK_ONLY)
    } else {
      nodes.zipWithIndex().persist(StorageLevel.DISK_ONLY)
    }
    val initMaps = rawMaps.map(ele => (new ReadOnlyByteArray(ele._1.getBytes()), ele._2))
    val totalNodeNum = initMaps.count()
    println(s"Total node number = $totalNodeNum")
    // Start PS and init the model
    LogUtils.logTime("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    initPSModel(totalNodeNum, initMaps)
    val startTs = System.currentTimeMillis()
    // Before training, checkpoint the model
    getPSModel.checkpointEmbeddingMatrix(0)
    LogUtils.logTime(s"Write checkpoint use time=${System.currentTimeMillis() - startTs}")

    val beforeReindex = System.currentTimeMillis()
    val edges = rawEdges.map(ele => (new ReadOnlyByteArray(ele._1.getBytes()), new ReadOnlyByteArray(ele._2.getBytes())))
    val results = getPSModel.getValues(edges, batchSize)
    println("Total edge number = " + results.count)
    LogUtils.logTime(s"Reindex finished, cost ${(System.currentTimeMillis() - beforeReindex) / 1000.0}s")

    val rawMapsDF = SparkSession.builder().getOrCreate().createDataFrame(rawMaps.map { case (oid, nid) =>
      Row(oid, nid)
    }, StructType(Seq(StructField("oid", StringType, nullable = false), StructField("nid", LongType, nullable = false))))
    val resultsDF = SparkSession.builder().getOrCreate().createDataFrame(results.map { case (snid, dnid) =>
      Row(snid, dnid)
    }, StructType(Seq(StructField("snid", LongType, nullable = false), StructField("dnid", LongType, nullable = false))))
    (rawMapsDF, resultsDF)

  }

  def initPSModel(nodeNum: Long, maps: RDD[(ReadOnlyByteArray, Long)]) = {
    val modelContext = new ModelContext(
      psPartitionNum, -1, -1, nodeNum, "mapping", SparkContext.getOrCreate().hadoopConfiguration)

    psModel = ReIndexPSModel(modelContext)
    psModel.initialize(maps, batchSize)
  }

  def getPSModel = psModel
}

class ReIndexModelWithWeight(dataset: Dataset[_], dataPartitionNum: Int, psPartitionNum: Int, srcNodeIdCol: String,
                             dstNodeIdCol: String, weightCol: String, batchSize: Int) extends ReIndexModel(dataset, dataPartitionNum,
  psPartitionNum, srcNodeIdCol, dstNodeIdCol, batchSize) {

  override def action(mapsInput: RDD[(String, Long)]=null): (DataFrame, DataFrame) = {
    // Original edges
    val rawEdges: RDD[(String, String, Float)] = dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd
      .map(row => (row.getString(0), row.getString(1), row.getFloat(2)))
      .filter(f => !f._1.equals(f._2)).filter(f => f._3 != 0)
    rawEdges.persist(StorageLevel.DISK_ONLY)

    val nodes = rawEdges.flatMap(x => Seq(x._1, x._2)).distinct(dataPartitionNum)
    val rawMaps = if (mapsInput != null) {
      val mapsInputCount = mapsInput.count()
      val nodesInputRDD = mapsInput.map(f => f._1)
      val nodeSubtract = nodes.subtract(nodesInputRDD)
      println(s"previous mapping nodes num=$mapsInputCount, increasing nodes num=${nodeSubtract.count()}")

      val nodeSubtractWithIndex = nodeSubtract.zipWithIndex().map(ele => (ele._1, ele._2 + mapsInputCount))
      mapsInput.union(nodeSubtractWithIndex).persist(StorageLevel.DISK_ONLY)
    } else {
      nodes.zipWithIndex().persist(StorageLevel.DISK_ONLY)
    }
    val totalNodeNum = rawMaps.count()
    println(s"Total node number = $totalNodeNum")
    // Start PS and init the model
    LogUtils.logTime("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    initPSModel(totalNodeNum, rawMaps.map(ele => (new ReadOnlyByteArray(ele._1.getBytes()), ele._2)))
    val startTs = System.currentTimeMillis()
    // Before training, checkpoint the model
    getPSModel.checkpointEmbeddingMatrix(0)
    LogUtils.logTime(s"Write checkpoint use time=${System.currentTimeMillis() - startTs}")

    val beforeReindex = System.currentTimeMillis()
    val edges = rawEdges.map(ele => (new ReadOnlyByteArray(ele._1.getBytes()), new ReadOnlyByteArray(ele._2.getBytes()), ele._3))
    val results = getPSModel.getValuesWithWeight(edges, batchSize)
    println("Total edge number = " + results.count)
    LogUtils.logTime(s"Reindex finished, cost ${(System.currentTimeMillis() - beforeReindex) / 1000.0}s")
    val rawMapsDF = SparkSession.builder().getOrCreate().createDataFrame(rawMaps.map { case (oid, nid) =>
      Row(oid, nid)
    }, StructType(Seq(StructField("oid", StringType, nullable = false), StructField("nid", LongType, nullable = false))))
    val resultsDF = SparkSession.builder().getOrCreate().createDataFrame(results.map { case (snid, dnid, weight) =>
      Row(snid, dnid, weight)
    }, StructType(Seq(StructField("snid", LongType, nullable = false), StructField("dnid", LongType, nullable = false),
      StructField("weight", FloatType, nullable = false))))
    (rawMapsDF, resultsDF)
  }
}
