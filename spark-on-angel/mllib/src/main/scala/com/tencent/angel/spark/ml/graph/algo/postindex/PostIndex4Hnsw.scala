package com.tencent.angel.spark.ml.graph.algo.postindex

import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


class PostIndex4Hnsw(override val uid: String)  extends Serializable with HasPartitionNum with HasIsOldFirst
  with HasStorageLevel {

  def this() = this(Identifiable.randomUID("POSTINDEX4HNSW"))

  def transform(input: Dataset[_], mapping: Dataset[_]): DataFrame = {
    val mapsInputTmp = if ($(isOldFirst)) mapping.select("dst", "src").rdd
      .filter(row => !row.anyNull)
      .map(f => (f.getString(0).toLong, f.getString(1)))
      .repartition($(partitionNum))
      .persist($(storageLevel))
    else mapping.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(f => (f.getString(0).toLong, f.getString(1)))
      .repartition($(partitionNum))
      .persist($(storageLevel))
    val nodesNum = mapsInputTmp.count()
    println(s"$nodesNum nodes in mapping file.")

    val maps = SparkContext.getOrCreate().broadcast(mapsInputTmp.collectAsMap())
    val inputRDD = input.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(f => (f.getLong(0), f.getString(1).split( " ").map{dis =>
        val temp = dis.split(":")
        (temp(0).toLong, temp(1).toFloat)
      }))
      .repartition($(partitionNum))
      .persist($(storageLevel))
    val inputNum = inputRDD.count()
    println(s"$inputNum records in input file.")

    println(s"start to make saving data")
    val timeMakeSaving = System.currentTimeMillis()
    val retRDD = inputRDD.mapPartitionsWithIndex((idx, iter) =>
      Iterator.single((idx, iter.toArray)).flatMap { case (_, sentences) =>
        sentences.map { line =>
          val values = line._2.map(nbr => (maps.value.getOrElse(nbr._1, ""), nbr._2)).filter(r => r._1 != "")
          (maps.value.getOrElse(line._1, ""), values.map(nbr => s"${nbr._1}:${nbr._2}").mkString(" "))
        }
      }
    ).filter(row => row._1 != "")
      .map(row => Row.fromSeq(Seq[Any](row._1, row._2)))
    println(s"make saving data cost: ${System.currentTimeMillis() - timeMakeSaving} ms")

    val schema = StructType(Seq(
      StructField("query", StringType, nullable = false),
      StructField("nbr:dis", StringType, nullable = false)))
    val spark = SparkSession.builder.getOrCreate
    spark.createDataFrame(retRDD, schema)
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
