package com.tencent.angel.graph.embedding.eges

import java.util.{HashMap => JHashMap, HashSet => JHashSet}

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object EGESOps {
  /* load Node Ids and Side Information Ids */
  def EGESDataProcess(ss: SparkSession, input: String, output: String, dataPartNum: Int, numSideInfo: Int):
  (RDD[Array[Int]], Int, Int, Int, Int) = {
    deleteIfExists(output + "/itemMappingTab", ss)
    deleteIfExists(output + "/sideInfoMappingTab", ss)

    val data: RDD[String] = ss.sparkContext.textFile(input).repartition(dataPartNum)
      .filter(line => line != null && line.length > 0)
      .map(line => (line, 1)).reduceByKey(_ + _).map(line => line._1)
    data.persist(StorageLevel.DISK_ONLY)

    // do remapping to the tables
    val tempNeigh = corpusStringToInt(data)
    val (remapTab, itemMappingTab, sideInfoMappingTab) = (tempNeigh._1, tempNeigh._2, tempNeigh._3)
    saveMap(itemMappingTab, output + "/itemMappingTab", " ")
    saveMap(sideInfoMappingTab, output + "/sideInfoMappingTab", " ")

    val remapNeiTab = remapTab.map(line => line.slice(0,2))
    remapNeiTab.persist(StorageLevel.DISK_ONLY)
    remapNeiTab.count()
    val (minNodeId, maxNodeId) = computeMaxMinId(remapNeiTab)
    val (minId, maxId) = computeMaxMinId(remapTab)

    remapNeiTab.unpersist()
    data.unpersist()
    (remapTab, minNodeId, maxNodeId, minId, maxId)

  }

  def EGESDataProcessWithoutRemap(ss: SparkSession, input: String, dataPartNum: Int, numSideInfo: Int):
  (RDD[Array[Int]], Int, Int, Int, Int) = {
    // read input data and preProcess
    val data: RDD[Array[Int]] = ss.sparkContext.textFile(input).repartition(dataPartNum).filter(line =>
      line != null && line.length > 0).map(line => (line, 1)).reduceByKey(_ + _).map(line => line._1)
      .map(line => line.stripLineEnd.split("[\\s+|,]")).filter(line => line.length == numSideInfo + 2)
      .map(e => e.map(v => v.toInt))
    data.persist(StorageLevel.DISK_ONLY)
    println(s"the number of lines of input data after processing is ${data.count()}.")
    val minNodeId = data.map(e => e.slice(0,2).min).min()
    val maxNodeId = data.map(e => e.slice(0,2).max).max()
    val minId = data.map(e => e.min).min()
    val maxId = data.map(e => e.max).max()
    data.unpersist()
    (data, minNodeId, maxNodeId, minId, maxId)
  }

  def deleteIfExists(modelPath: String, ss: SparkSession): Unit = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def recalWeights(input: String, ss: SparkSession, numWeightsSI: Int): RDD[(Int, Array[Float])] = {
    val weights: RDD[(Int, Array[Float])] = ss.sparkContext.textFile(input).map(line =>
      line.stripLineEnd.split("[\\s+|,|:]")).map(line => (line(0).toInt, line.slice(1, numWeightsSI+1)
      .map(e => e.toFloat))).map{
      line =>
        val sum = line._2.map(e => math.exp(e)).sum.toFloat
        val lineWeights = line._2.map(e => e / sum)
        (line._1, lineWeights)
    }
    val num = weights.count()
    println(s"the number of groups of weights is $num.")
    weights
  }

  def saveMap(scores: RDD[(Int, String)],
              path: String,
              seq: String = " "): Unit = {
    scores.map(f => s"${f._1}$seq${f._2}")
      .saveAsTextFile(path)
  }

  def saveRemap(scores: RDD[Array[Int]],
                path: String,
                seq: String = " "): Unit = {
    scores.map{
      case line =>
        var strings = s"${line(0)}$seq"
        for (i <- 1 until line.length) {
          strings = strings ++ s"${line(i)}$seq"
        }
        strings
    }.saveAsTextFile(path)
  }

  def save(scores: RDD[(Int, Array[Float])],
           path: String,
           seq: String = " "): Unit = {
    scores.map{
      case line =>
        var strings = s"${line._1}$seq"
        for (i <- 0 until line._2.length) {
          strings = strings ++ s"${line._2(i)}$seq"
        }
        strings
    }.saveAsTextFile(path)
  }

  def corpusStringToInt(data: RDD[String]): (RDD[Array[Int]], RDD[(Int, String)], RDD[(Int, String)]) = {

    // All distinct strings
    val strings = data.filter(f => f != null && f.length > 0)
      .map(f => f.stripLineEnd.split("[\\s+|,]"))

    val stringsEdge = strings.map(e => e.slice(0,2)).flatMap(e => e).map(e => (e, 1)).reduceByKey(_ + _).map(f => f._1)
    val stringsSideInfo = strings.map(e => e.slice(2,e.length)).flatMap(e => e)
      .map(e => (e, 1)).reduceByKey(_ + _).map(f => f._1)

    val stringsEdgeWithIndex = stringsEdge.zipWithIndex().cache()
    val maxNodeId = stringsEdgeWithIndex.map(e => e._2).max()
    val stringsSideInfoWithIndex = stringsSideInfo.zipWithIndex().map(e => (e._1, e._2 + maxNodeId + 1)).cache()

    def buildRoutingTable(index: Int, iterator: Iterator[String]): Iterator[(String, Int)] = {
      val set = new JHashSet[String]()
      while (iterator.hasNext) {
        val line = iterator.next()
        if (line != null && line.length > 0) {
          for (word <- line.stripLineEnd.split("[\\s+|,]")) {
            set.add(word)
          }
        }
      }

      val it = set.iterator()
      val result = new ArrayBuffer[(String, Int)]()
      while (it.hasNext) {
        result.append((it.next(), index))
      }

      result.iterator
    }

    val routingTable = data.mapPartitionsWithIndex((partId, iterator) =>
      buildRoutingTable(partId, iterator), true)

    val partIndexEdge = routingTable.join(stringsEdgeWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(data.getNumPartitions)

    val partIndexSideInfo = routingTable.join(stringsSideInfoWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(data.getNumPartitions)

    def attachPartitionId(index: Int, iterator: Iterator[String]): Iterator[(Int, Array[String])] = {
      Iterator.single((index, iterator.toArray))
    }

    val ints = data.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true).join(partIndexEdge).map { case (_, (sentences, mapping)) =>
      val map = new JHashMap[String, Long]()
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      sentences.filter(f => f != null && f.length > 0).map { case line =>
        line.stripLineEnd.split("[\\s+|,]")
      }.map(e => e.slice(0,2).map(s => map.get(s).toString) ++ e.slice(2,e.length))
    }.flatMap(f => f).map(e => e.mkString(" "))

    val ints2 = ints.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true).join(partIndexSideInfo).map { case (_, (sentences, mapping)) =>
      val map = new JHashMap[String, Long]()
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      sentences.filter(f => f != null && f.length > 0).map { case line =>
        line.stripLineEnd.split("[\\s+|,]")
      }.map(e => e.slice(0,2).map(s => s.toInt) ++ e.slice(2,e.length).map(s => map.get(s).toInt))
    }.flatMap(f => f)

    (ints2, stringsEdgeWithIndex.map(f => (f._2.toInt, f._1)), stringsSideInfoWithIndex.map(f => (f._2.toInt, f._1)))
  }

  def summarizeApplyOp(iterator: Iterator[Array[Int]]): Iterator[(Int, Int)] = {
    var minId = Int.MaxValue
    var maxId = Int.MinValue
    while (iterator.hasNext) {
      val entry = iterator.next()
      minId = math.min(minId, entry.min)
      maxId = math.max(maxId, entry.max)
    }

    Iterator.single((minId, maxId))
  }

  def summarizeReduceOp(t1: (Int, Int),
                        t2: (Int, Int)): (Int, Int) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2))

  def computeMaxMinId(data: RDD[Array[Int]]): (Int, Int) = {
    val (minId, maxId) = data.mapPartitions(summarizeApplyOp)
      .reduce(summarizeReduceOp)
    (minId, maxId)
  }
}
