package com.tencent.angel.spark.ml.ann.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object DataUtils {
  private val DELIMITER = "delimiter"
  private val HEADER = "header"

  def loadRawItems(ss: SparkSession, path: String, partNum: Int,
                   itemSep: String = ":", vecSep: String = " "): DataFrame = {

    val sc = ss.sparkContext

    val rawData = sc.textFile(path, partNum)
      .map(line => line.trim.split(itemSep))
      .map { lis =>
        val len = lis.length
        val vec = lis.slice(1, len)
        (lis(0), vec.mkString(vecSep))
      }

    val schema = StructType(Seq(
      StructField("item", StringType, nullable = false),
      StructField("vector", StringType, nullable = false)
    ))

    val rowRDD = rawData.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

    ss.createDataFrame(rowRDD, schema)
  }

  def loadVectors(ss: SparkSession, path: String, partNum: Int,
                  itemSep: String = ":", vecSep: String = " ", itemIdx: Int = 0, vecIdx: Int = 1): DataFrame = {
    ; {
      val schema = StructType(Seq(
        StructField("item", StringType, nullable = false),
        StructField("vector", StringType, nullable = false)))

      ss.read
        .option("sep", itemSep)
        .option("header", "false")
        .schema(schema)
        .csv(path)
    
}

  }


  def loadVectorsMultilevel(sc: SparkContext, path: String, partNum: Int,
                            itemSep: String = ":", vecSep: String = " ", itemIdx: Int = 0, vecIdx: Int = 1): DataFrame = {
    val schema = StructType(Seq(
      StructField("item", StringType, nullable = false),
      StructField("vector", StringType, nullable = false)))

    val inputRDD = sc.textFile(path).map(line => line.trim.split(itemSep))
    val ss = SparkSession.builder().getOrCreate()
    val result = ss.createDataFrame(inputRDD.map(line => Row.fromSeq(line)), StructType(schema))
    result
  }

  def loadVectorsWithClusterIDMultilevel(sc: SparkContext, path: String, partNum: Int,
                                         itemSep: String = ":", vecSep: String = " ", itemIdx: Int = 0, vecIdx: Int = 1, clusterIdx: Int = 2): DataFrame = {
    val schema = StructType(Seq(
      StructField("item", StringType, nullable = false),
      StructField("vector", StringType, nullable = false),
      StructField("clusterID", StringType, nullable = false)))

    val inputRDD = sc.textFile(path).map(line => line.trim.split(itemSep)).repartition(partNum)
    val ss = SparkSession.builder().getOrCreate()
    val result = ss.createDataFrame(inputRDD.map(line => Row.fromSeq(line)), StructType(schema))
    result
  }


  def loadIdQueries(ss: SparkSession, path: String, partNum: Int, sep: String = " "): RDD[Long] = {
    val sc = ss.sparkContext

    val qIds = sc.textFile(path, partNum)
      .map(line => line.trim.split(sep))
      .flatMap(lis => lis.map(_.toLong))
      .repartition(partNum)

    qIds
  }

  def saveResult(df: DataFrame, path: String, sep: String = "\t"): Unit = {
    df.printSchema()
    ; {
      df.write
        .mode(SaveMode.Overwrite)
        .option(HEADER, "false")
        .option(DELIMITER, sep)
        .csv(path)
    
}
  }

  def loadKNNResults(ss: SparkSession, path: String, sep: String = "\t"): DataFrame = {
    ; {
      val schema = StructType(Seq(
        StructField("query", StringType, nullable = false),
        StructField("nbr:dis", StringType, nullable = false)))

      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(path)
    
}
  }





  def loadAndReindexFromDF(dataset: Dataset[_],
                           itemIdCol: String,
                           vectorCol: String,
                           vecSep: String,
                           partitionNum: Int): (RDD[(Long, Array[Float])], RDD[(Long, String)]) = {
    val rawData = dataset.select(itemIdCol, vectorCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getString(0), row.getString(1).split(vecSep).map(_.toFloat)))
      .repartition(partitionNum)

    val numVectors = rawData.count()
    println(s"load $numVectors vectors ... start reindexing ...")

    val dataWithId = rawData.zipWithUniqueId()
    val data = dataWithId.map(t => (t._2, t._1._2))
    val indexMap = dataWithId.map(t => (t._2, t._1._1))

    (data, indexMap)
  }

  def loadFromDF(dataset: Dataset[_],
                 itemIdCol: String,
                 vectorCol: String,
                 vecSep: String,
                 partitionNum: Int): RDD[(Long, Array[Float])] = {
    val data = dataset.select(itemIdCol, vectorCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getString(0).toLong, row.getString(1).split(vecSep).map(_.toFloat)))
      .repartition(partitionNum)

    data
  }

  def loadFromDFWithClusterID(dataset: Dataset[_],
                              itemIdCol: String,
                              vectorCol: String,
                              clusterID: String,
                              vecSep: String,
                              partitionNum: Int): RDD[(Long,   Array[(Long, Array[Float])])] = {
    val data = dataset.select(clusterID, itemIdCol, vectorCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getString(0).toLong, (row.getString(1).toDouble.toLong, row.getString(2).split(vecSep).map(_.toFloat)))).groupByKey().map(a=>(a._1,a._2.toArray))
      .repartition(partitionNum)
    data
  }

  def loadOriQueryFromDF(dataset: Dataset[_],
                              itemIdCol: String,
                              vectorCol: String,
                              vecSep: String,
                              partitionNum: Int):  RDD[(Long, Array[Float])] = {
    val data = dataset.select(itemIdCol, vectorCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getString(0).toDouble.toLong,  row.getString(1).split(vecSep).map(_.toFloat)))
    data
  }


}
