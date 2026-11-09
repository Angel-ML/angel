package com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition

import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

import scala.reflect.ClassTag


object String2IntReIndex {

  def main(args: Array[String]): Unit = {
    val output = "E:\\angel-things\\Graph\\decoded"
    val input = "E:\\angel-things\\Graph\\test"
    val mapPath = "E:\\angel-things\\Graph\\map"
    start("local")
    val rawEdges = GraphIO.loadInt(input, true, sep="\t")
      .select("src", "dst", "weight").rdd
      .map(x => (x.getInt(0), x.getInt(1), x.getFloat(2)))
    val decoded = decodeEdge(rawEdges, mapPath)
    val schema = StructType(Seq(
      StructField(s"src", StringType, nullable = false),
      StructField(s"dst", StringType, nullable = false),
      StructField(s"weight", FloatType, nullable = false)
    ))
    val ss = SparkSession.builder().getOrCreate()
    GraphIO.save(ss.createDataFrame(decoded.map(x => Row(x._1, x._2, x._3)), schema), output)
    stop()
  }

  def encodeEdge[U: ClassTag](edges: RDD[(String, String, U)], mapPath: String): RDD[(Int, Int, U)] = {
    val rawNodes = edges.flatMap(x => Iterator((x._1, 1), (x._2, 1))).reduceByKey(_ + _).map(_._1)
    val string2Int = rawNodes.zipWithIndex().map(x => (x._1, x._2.toInt)).cache()
    val schema = StructType(Seq(
      StructField(s"String", StringType, nullable = false),
      StructField(s"Int", IntegerType, nullable = false)
    ))
    val ss = SparkSession.builder().getOrCreate()
    GraphIO.save(ss.createDataFrame(string2Int.map(x => Row(x._1, x._2)), schema), mapPath)
    println(s"saved string2int map to $mapPath.")

    val encoded = edges.map(x => (x._1, (x._2, x._3))).join(string2Int)
      .map { case (_, ((dst, attr), src)) => (dst, (src, attr))}
      .join(string2Int)
      .map { case (_, ((src, attr), dst)) => (src, dst, attr)}
    encoded
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("deepWalk")
    val sc = new SparkContext(conf)
    //    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

  def encodeEdge(edges: RDD[(String, String)], mapPath: String): RDD[(Int, Int)] = {
    val rawNodes = edges.flatMap(x => Iterator((x._1, 1), (x._2, 1))).reduceByKey(_ + _).map(_._1)
    val string2Int = rawNodes.zipWithIndex().map(x => (x._1, x._2.toInt)).cache()

    val schema = StructType(Seq(
      StructField(s"String", StringType, nullable = false),
      StructField(s"Int", IntegerType, nullable = false)
    ))
    val ss = SparkSession.builder().getOrCreate()
    GraphIO.save(ss.createDataFrame(string2Int.map(x => Row(x._1, x._2)), schema), mapPath)
    println(s"saved string2int map to $mapPath.")

    val encoded = edges.join(string2Int)
      .map { case (_, (dst, src)) => (dst, src)}
      .join(string2Int)
      .map { case (_, (src, dst)) => (src, dst)}
    encoded
  }

  def decodeEdge(rdd: RDD[(Int, Int)], mapPath: String): RDD[(String, String)] = {
    val map = GraphIO.loadString2IntMap(mapPath, sep = "\t")
      .select("src", "dst").rdd
      .map(row => (row.getInt(1), row.getString(0)))
      .cache()

    val decoded = rdd.join(map)
        .map { case (_, (dst, src)) => (dst, src)}
        .join(map)
        .map { case (_, (src, dst)) => (src, dst)}
    decoded
  }

  def decodeEdge[U: ClassTag](rdd: RDD[(Int, Int, U)], mapPath: String): RDD[(String, String, U)] = {
    val map = GraphIO.loadString2IntMap(mapPath, sep = "\t")
      .select("src", "dst").rdd
      .map(row => (row.getInt(1), row.getString(0)))
      .cache()

    val decoded = rdd.map(x => (x._1, (x._2, x._3))).join(map)
      .map { case (_, ((dst, attr), src)) => (dst, (src, attr))}
      .join(map)
      .map { case (_, ((src, attr), dst)) => (src, dst, attr)}
    decoded
  }

  def decodeNode(rdd: RDD[Int],  mapPath: String): RDD[String] = {
    val map = GraphIO.loadString2IntMap(mapPath, sep = "\t")
      .select("src", "dst").rdd
      .map(row => (row.getInt(1), row.getString(0)))
      .cache()

    val decoded = rdd.map(x => (x, 1)).join(map)
      .map { case (_, (_, src)) => src}
    decoded
  }

  def decodeNode[U: ClassTag](rdd: RDD[(Int, U)], mapPath: String): RDD[(String, U)] = {
    val map = GraphIO.loadString2IntMap(mapPath, sep = "\t")
      .select("src", "dst").rdd
      .map(row => (row.getInt(1), row.getString(0)))
      .cache()

    val decoded = rdd.join(map)
      .map { case (_, (attr, src)) => (src, attr)}
    decoded
  }

}
