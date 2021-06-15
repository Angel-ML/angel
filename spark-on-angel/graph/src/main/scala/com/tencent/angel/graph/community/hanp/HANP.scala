package com.tencent.angel.graph.community.hanp

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.graph.community.OverlapNMI
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class HANP(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasMaxIteration with HasPreserveRate with HasDelta with HasIsWeighted
  with HasWeightCol {


  def this() = this(Identifiable.randomUID("HANP"))


  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = if ($(isWeighted)) {
      dataset.select(${srcNodeIdCol}, ${dstNodeIdCol}, ${weightCol}).rdd
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2)
        .filter(f => f._3 != 0)
        .flatMap(f => Iterator((f._1, (f._2, f._3)), (f._2, (f._1, f._3))))
    } else {
      dataset.select(${srcNodeIdCol}, ${dstNodeIdCol}).rdd
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(f => f._1 != f._2)
        .flatMap(f => Iterator((f._1, (f._2, f._3)), (f._2, (f._1, f._3))))
    }
    edges.persist($(storageLevel))

    val edgesGroup = edges.map(e => (e._1, e._2._1)).groupByKey($(partitionNum))
      .map(e => (e._1, e._2.toArray))
    val stats = NeighborDataOps.statsByNeighborTable(edgesGroup)
    println(s"min node id = ${stats._1}")
    println(s"max node id = ${stats._2}")
    println(s"num of nodes = ${stats._3}")
    println(s"num of edges = ${stats._4}")
    println(s"minDegree = ${stats._5}")
    println(s"maxDegree = ${stats._6}")

    val maxId = stats._2 + 1
    val minId = stats._1

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = HANPPSModel.fromMinMax(minId, maxId, $(psPartitionNum), $(useBalancePartition))

    var graph = edges.groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator(HANPGraphPartition.apply(index, it, ${delta}, ${preserveRate})))
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.foreach(_.initDegrees(model))
    graph.foreach(_.initLabels(model))
    graph.foreach(_.initScores(model))

    var curIteration = 0
    var prev = graph

    do {
      curIteration += 1
      graph = prev.map(_.process(model))
      graph.persist($(storageLevel))
      graph.count()
      prev.unpersist(true)
      prev = graph
      model.resetMsgs()
      println(s"curIteration=$curIteration")
    } while (curIteration < ${maxIteration})

    def calcNMI(realCommunitiesFile: String, overlapCommunities: RDD[(Long, Array[Long])]):
    Unit = {
      // 获取真实社区信息
      def getRealCommunities(realCommunitiesFile: String) : (RDD[(String, Array[Long])],
        RDD[(Long,Array[String])]) = {
        val node2com = SparkContext.getOrCreate().textFile(realCommunitiesFile)
          .map(x => x.trim.split("\t"))
          .map(x => (x(0).toLong, x(1).split(" ")))
          .persist(StorageLevel.MEMORY_AND_DISK_SER).setName("node2com")

        val com2node = node2com.flatMap(item => {
          var res = new mutable.ArrayBuffer[(String,Array[Long])]
          val (vid, labels) = item
          labels.foreach(label => {res += ((label, Array(vid)))})
          res
        }).reduceByKey(_++_).persist(StorageLevel.MEMORY_AND_DISK_SER).setName("com2node")
        ( com2node, node2com)
      }

      val (realCommunities, vidLabels) = getRealCommunities(realCommunitiesFile)
      val communitySize = overlapCommunities.mapPartitions(iter => for((label, nodes) <- iter)
        yield (label, nodes.size))
        .persist(StorageLevel.MEMORY_AND_DISK_SER).setName("communitySize")
      val nmi = new OverlapNMI()
      val com = overlapCommunities

      val x = com.flatMap(item => {
        var res = new mutable.ArrayBuffer[(Long,Array[Long])]
        val (vid, labels) = item
        labels.foreach(label => {res += ((label, Array(vid)))})
        res
      }).reduceByKey(_++_).sortByKey()
      val y = realCommunities.sortByKey().map(x=>x._2)

      val verticesNum = overlapCommunities.count()
      val nmi_max = nmi.NMI_max(x.map(_._2), y, verticesNum)
      val nmi_flk = nmi.NMI_LFK(x.map(_._2), y, verticesNum)
      val (pureRate, _) = nmi.pureRate(overlapCommunities, vidLabels, communitySize)
      println(s"verticesNum: $verticesNum, NMI_MAX: $nmi_max, NMI_LFK: $nmi_flk, " +
        s"pureRate: $pureRate")
    }

    val temp = graph.map(_.save()).flatMap(f => f._1.zip(f._2))

    val retRDD = temp.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        $(outputCoreIdCol)
      }", LongType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
