package com.tencent.angel.spark.ml.graph.algo.statistics.hoplayout

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, LongParam, Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class SSHopLayout(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasOutputNodeIdCol with HasOutputDistanceCol
  with HasDstNodeIdCol with HasStorageLevel with HasPSPartitionNum
  with HasPartitionNum with HasUseBalancePartition {

  final val source = new LongParam(this, "source", "source")
  final def setSource(node: Long): this.type = set(source, node)
  
  final val singlecc = new BooleanParam(this, "singlecc", "singlecc")
  final def setSingleCC(isSingle: Boolean): this.type = set(singlecc, isSingle)
  setDefault(singlecc, false)
  
  final val ccinfopath = new Param[String](this, "ccinfopath", "ccinfopath")
  final def setCCInfoPath(iPath: String): this.type = set(ccinfopath, iPath)
  
  def this() = this(Identifiable.randomUID("sshoplayout"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    
    edges.persist(StorageLevel.MEMORY_ONLY_SER)
    
    val maxId = edges.map(e => math.max(e._1, e._2)).max() + 1
    val minId = edges.map(e => math.min(e._1, e._2)).min()
    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val numEdges = edges.count()
  
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val model = SSHopLayoutPSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition))
    val initialMsgs = VFactory.sparseLongKeyIntVector(model.dim)
    initialMsgs.set($(source), 0)
    model.initMsgs(initialMsgs)
    
    var nbrTable = edges.flatMap(e => Iterator((e._1, e._2), (e._2, e._1)))
      .groupByKey($(partitionNum))
  
    var processTable = nbrTable
    if (!$(singlecc)) {
      processTable = SSHopLayoutOperator.selfCCEdgesDetection($(ccinfopath), nbrTable, $(source), $(partitionNum))
    }
    
    var graph = processTable
      .mapPartitionsWithIndex((index, it) => Iterator.single(SSHopLayoutGraphPartition(index, it)))
    
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.count()
    
    var prevGraph = graph
    var curIteration = 0
    var numMsgs = model.numMsgs()
    var leftNodesNum = 0L
    do {
      curIteration += 1
      graph = prevGraph.map(_.process(model))
      graph.persist($(storageLevel))
      graph.count()
      prevGraph.unpersist(blocking = true)
      prevGraph = graph
      model.reset()
      numMsgs = model.numMsgs()
      leftNodesNum = graph.map(_.leftNodesCount()).reduce(_ + _)
      println(s"Iteration:$curIteration, numMsgs:$numMsgs, node left: $leftNodesNum")
    } while(leftNodesNum > 0)
  
    val retRDD = graph.map(_.save()).flatMap(f => f._1.zip(f._2))
      .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
  
    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(
      Seq(
        StructField($(srcNodeIdCol), LongType, nullable = false),
        StructField($(outputDistanceCol), IntegerType, nullable = false)
      )
    )
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
