package com.tencent.angel.graph.reindex

import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.graph.utils.params.{HasBatchSize, HasDstNodeIdCol, HasIsWeighted, HasPSPartitionNum,
  HasPartitionNum, HasReIndexedNodesMapPath, HasSrcNodeIdCol, HasWeightCol}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

class ReIndex (override val uid: String) extends Transformer
  with HasPartitionNum with HasPSPartitionNum with HasIsWeighted with HasSrcNodeIdCol
  with HasDstNodeIdCol with HasWeightCol with HasBatchSize with HasReIndexedNodesMapPath {

  def this() = this(Identifiable.randomUID("REINDEX"))

  /**
    * PS model
    */
  @volatile var model:ReIndexModel = _

  /**
    * final results
    */
  var results: (DataFrame, DataFrame) = (null, null)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val mapsInput = if ($(reIndexedNodesMapPath) != null && $(reIndexedNodesMapPath) != "") {
      val conf = SparkContext.getOrCreate().getConf
      val mapsSep = conf.get("spark.hadoop.angel.reindex.map.sep", "colon") match {
        case "space" => " "
        case "comma" => ","
        case "tab" => "\t"
        case "colon" => ":"
        case "bar" => "\\|"
      }
      val mapsInputTmp = GraphIO.loadString2IntMap($(reIndexedNodesMapPath), sep=mapsSep)
        .select("src", "dst")
        .rdd
        .map(r => (r.getString(0), r.getLong(1)))
        .persist(StorageLevel.DISK_ONLY)
      mapsInputTmp
    } else null

    if ($(isWeighted)) {
      model = new ReIndexModelWithWeight(dataset, $(partitionNum), $(psPartitionNum), $(srcNodeIdCol),
        $(dstNodeIdCol), $(weightCol), $(batchSize))
    } else {
      model = new ReIndexModel(dataset, $(partitionNum), $(psPartitionNum), $(srcNodeIdCol),
        $(dstNodeIdCol), $(batchSize))
    }
    results = model.action(mapsInput)
    dataset.sparkSession.emptyDataFrame
  }

  def save(output: String, mapPath: String, sep: String): Unit = {
    val conf = SparkContext.getOrCreate().getConf
    val mapsSep = conf.get("spark.hadoop.angel.reindex.map.sep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "\\|"
    }
    GraphIO.save(results._1, mapPath, mapsSep)
    GraphIO.save(results._2, output, sep)
  }

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???
}
