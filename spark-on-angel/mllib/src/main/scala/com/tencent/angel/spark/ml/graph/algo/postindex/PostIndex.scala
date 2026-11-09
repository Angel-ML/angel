package com.tencent.angel.spark.ml.graph.algo.postindex


import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


class PostIndex(override val uid: String)  extends Serializable with HasPartitionNum with HasIsOldFirst with HasStorageLevel {

  def this() = this(Identifiable.randomUID("POSTINDEX"))

  def transform(input: Dataset[_], mapping: Dataset[_]): DataFrame = {
    val mapsInputTmp = if ($(isOldFirst)) mapping.select("dst", "src").rdd
      .filter(row => !row.anyNull)
      .map(f => (f.getString(0), f.getString(1)))
      .repartition($(partitionNum))
      .persist($(storageLevel))
    else mapping.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(f => (f.getString(0), f.getString(1)))
      .repartition($(partitionNum))
      .persist($(storageLevel))
    val nodesNum = mapsInputTmp.count()
    println(s"$nodesNum nodes in mapping file.")

    val inputRDD = input.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(f => (f.getString(0), f.getString(1)))
      .repartition($(partitionNum))
      .persist($(storageLevel))
    val inputNum = inputRDD.count()
    println(s"$inputNum records in input file.")

    val output = inputRDD.join(mapsInputTmp)                             // (new_id, (emb, old_id))
      .map(r => Row.fromSeq(Seq[Any](r._2._2, r._2._1)))                               // (old_id, emb)
      .persist($(storageLevel))
    val outputNum = output.count()
    println(s"$outputNum records in output file.")

    val spark = SparkSession.builder.getOrCreate
    val schema = StructType(Seq(
      StructField("oid", StringType, nullable = false),
      StructField("embedding", StringType, nullable = false)
    ))
    spark.createDataFrame(output, schema)
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
