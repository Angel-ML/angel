package com.tencent.angel.spark.ml.graph.algo.embedding.word2vec

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.ml.graph.algo.embedding.line3.ReadOnlyByteArray
import com.tencent.angel.spark.ml.graph.algo.reindex.ReIndexPSModel

import java.util.{HashMap => JHashMap, HashSet => JHashSet}
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}
import it.unimi.dsi.fastutil.objects.{Object2IntOpenHashMap, Object2ObjectOpenHashMap}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object Word2VecUtils {

  def getRawMaps(data: RDD[String], minCount: Int, mapsInput: RDD[(String, Long)]=null, hasIndexColumn: Boolean=false): RDD[(String, Long)] ={
    // All distinct strings, filter min count words
    val strings = if (hasIndexColumn) data.filter(f => f != null && f.length > 0)
      .map(f => f.stripLineEnd.split("[\\s+|,]").tail).flatMap(f => f)
      .map(t => (t, 1)).reduceByKey(_ + _).filter(ele => ele._2 > minCount).map(f => f._1)
    else data.filter(f => f != null && f.length > 0)
      .map(f => f.stripLineEnd.split("[\\s+|,]")).flatMap(f => f)
      .map(t => (t, 1)).reduceByKey(_ + _).filter(ele => ele._2 > minCount).map(f => f._1)

    val stringsWithIndex = if (mapsInput != null) {
      val mapsInputCount = mapsInput.count().toInt
      val nodesInputRDD = mapsInput.map(f => f._1)
      val nodeSubtract = strings.subtract(nodesInputRDD)
      println(s"previous mapping nodes num=$mapsInputCount, increasing nodes num=${nodeSubtract.count()}")

      val nodeSubtractWithIndex = nodeSubtract.zipWithIndex().map(ele => (ele._1, ele._2 + mapsInputCount + 1))
      mapsInput.union(nodeSubtractWithIndex).cache()
    } else {
      strings.zipWithIndex().map(ele => (ele._1, ele._2 + 1)).cache()
    }
    stringsWithIndex
  }

  def initMappingMatrix(data: RDD[String], minCount: Int, psPartitionNum: Int, batchSize: Int,
                        mapsInput: RDD[(String, Long)]=null, hasIndexColumn: Boolean=false): (RDD[Array[Int]], RDD[(Int, String)]) = {
    val stringsWithIndex = Word2VecUtils.getRawMaps(data, minCount, mapsInput, hasIndexColumn)
    val totalNodeNum = stringsWithIndex.count()
    val modelContext = new ModelContext(
      psPartitionNum, -1, -1, totalNodeNum, "mapping",
      SparkContext.getOrCreate().hadoopConfiguration)
    val mappingModel = ReIndexPSModel(modelContext)
    mappingModel.initialize(stringsWithIndex.map(ele => (new ReadOnlyByteArray(ele._1.getBytes()), ele._2)), 5000)
    mappingModel.checkpointEmbeddingMatrix(0)
    val corpus = if (hasIndexColumn) {
      val strings = data.filter(f => f != null && f.nonEmpty).map{f =>
        val temp = f.stripLineEnd.split("[\\s+|,]")
        (temp.head.toInt, temp.tail.map(e => new ReadOnlyByteArray(e.getBytes())))
      }
      val corpus = mappingModel.getValuesWithSentences(strings.map(s => s._2), batchSize)
      strings.map(s => s._1).zip(corpus).map(s => Array(s._1) ++ s._2)
    } else {
      val strings = data.filter(f => f != null && f.nonEmpty)
        .map(f => f.stripLineEnd.split("[\\s+|,]")).map(x => x.map(e => new ReadOnlyByteArray(e.getBytes())))
      mappingModel.getValuesWithSentences(strings, batchSize)
    }
    (corpus, stringsWithIndex.map(x => (x._2.toInt, x._1)))
  }

  def corpusStringToInt(data: RDD[String], minCount: Int, mapsInput: RDD[(String, Long)]=null, hasIndexColumn: Boolean=false): (RDD[Array[Int]], RDD[(Int, String)]) = {

    val stringsWithIndex = getRawMaps(data, minCount, mapsInput, hasIndexColumn)

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
      buildRoutingTable(partId, iterator),
      true)

    val partIndex = routingTable.join(stringsWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(data.getNumPartitions)

    def attachPartitionId(index: Int, iterator: Iterator[String]): Iterator[(Int, Array[String])] = {
      Iterator.single((index, iterator.toArray))
    }

    val ints = if (hasIndexColumn) data.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true).join(partIndex).map { case (_, (sentences, mapping)) =>
      val map = new JHashMap[String, Long]()
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      sentences.filter(f => f != null && f.length > 0).map{line =>
        val temp = line.stripLineEnd.split("[\\s+|,]")
        Array(temp.head.toInt) ++ temp.tail.map(s => map.get(s).toInt).filter(ele => ele > 0)}
    }.flatMap(f => f)
    else data.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true).join(partIndex).map { case (_, (sentences, mapping)) =>
      val map = new JHashMap[String, Long]()
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      sentences.filter(f => f != null && f.length > 0).map(line =>
        line.stripLineEnd.split("[\\s+|,]").map(s => map.get(s).toInt).filter(ele => ele > 0))
    }.flatMap(f => f)


    (ints, stringsWithIndex.map(f => (f._2.toInt, f._1)))
  }

  def corpusStringToInt(data: RDD[String], stringsWithIndex: RDD[(String, Int)]): RDD[(String, Array[Int])] = {

    def buildRoutingTable(index: Int, iterator: Iterator[String]): Iterator[(String, Int)] = {
      val set = new JHashSet[String]()
      while (iterator.hasNext) {
        val line = iterator.next()
        if (line != null && line.length > 0) {
          val splits = line.stripLineEnd.split("[\\s+|,]")
          for (word <- splits.slice(1, splits.length)) {
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
      buildRoutingTable(partId, iterator),
      true)

    val partIndex = routingTable.join(stringsWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(data.getNumPartitions)

    def attachPartitionId(index: Int, iterator: Iterator[String]): Iterator[(Int, Array[String])] = {
      Iterator.single((index, iterator.toArray))
    }

    val intsWithDocId = data.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true).join(partIndex).map { case (_, (sentences, mapping)) =>
      val map = new JHashMap[String, Long]()
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      sentences.filter(f => f != null && f.length > 0).map{ line =>
        val splits = line.stripLineEnd.split("[\\s+|,]")
        (splits(0), splits.slice(1, splits.length).map(s => map.get(s).toInt).filter(ele => ele > 0))
      }
    }.flatMap(f => f)
    intsWithDocId
  }

  def corpusStringToIntWithoutRemapping(data: RDD[String], minCount: Int): RDD[Array[Int]] = {
    if (minCount > 0) {
      // All distinct strings, filter min count words
      val strings = data.filter(f => f != null && f.length > 0)
        .map(f => f.stripLineEnd.split("[\\s+|,]")).flatMap(f => f)
        .map(t => (t, 1)).reduceByKey(_ + _).filter(ele => ele._2 > minCount).map(f => f._1)

      val stringsWithIndex = strings.zipWithIndex().map(ele => (ele._1, ele._2 + 1)).cache()


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
        buildRoutingTable(partId, iterator),
        true)

      val partIndex = routingTable.join(stringsWithIndex).map { case (str, (partId, index)) =>
        (partId, (str, index))
      }.groupByKey(data.getNumPartitions)

      def attachPartitionId(index: Int, iterator: Iterator[String]): Iterator[(Int, Array[String])] = {
        Iterator.single((index, iterator.toArray))
      }

      val ints = data.mapPartitionsWithIndex((partId, iterator) =>
        attachPartitionId(partId, iterator),
        true).join(partIndex).map { case (_, (sentences, mapping)) =>
        val map = new JHashMap[String, Long]()
        for ((string, index) <- mapping) {
          map.put(string, index)
        }

        sentences.filter(f => f != null && f.length > 0).map(line =>
          line.stripLineEnd.split("[\\s+|,]").filter(s => map.get(s).toInt > 0).map(ele => ele.toInt))
      }.flatMap(f => f)
      ints
    } else {
      data.filter(f => f != null && f.length > 0)
        .map(f => f.stripLineEnd.split("[\\s+|,]").map(s => s.toInt))
    }
  }

  def corpusStringToIntWithoutRemappingWithDocId(data: RDD[String]): RDD[(String, Array[Int])] = {
    data.filter(f => f != null && f.length > 0)
      .map{f =>
        val splits = f.stripLineEnd.split("[\\s+|,]")
        (splits(0), splits.slice(1, splits.length).map(s => s.toInt))
      }
  }

  def arraysDot(x: Array[Float], y: Array[Float]): Float = {
    var dotValue = 0.0f
    x.indices.foreach(i => dotValue += x(i) * y(i))
    dotValue
  }

  def axpy(y: Array[Float], x: Array[Float], a: Float): Unit = {
    x.indices.foreach(i => y(i) += a * x(i))
  }

  def div(x: Array[Float], f: Float): Unit = {
    x.indices.foreach(i => x(i) = x(i) / f)
  }

  def inc(dst: Array[Float], src: Array[Float]): Unit = {
    for (i <- dst.indices) {
      dst(i) += src(i)
    }
  }

  def srcNegativeMerge(inputUpdateCounter: Int2IntOpenHashMap, inputUpdates: Int2ObjectOpenHashMap[(Array[Float], Array[Float])],
                       nodeId: Int, g: Float, update: Array[Float]): Int = {
    var grads: Array[Float] = inputUpdates.get(nodeId)._2
    if (grads == null) {
      grads = new Array[Float](update.length)
      inputUpdates.put(nodeId, (inputUpdates.get(nodeId)._1, grads))
      inputUpdateCounter.put(nodeId, 0)
    }
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }

  def srcNegativeMerge(inputUpdateCounter: Object2IntOpenHashMap[String], inputUpdates: Object2ObjectOpenHashMap[String, (Array[Float], Array[Float])],
                       nodeId: String, g: Float, update: Array[Float]): Int = {
    var grads: Array[Float] = inputUpdates.get(nodeId)._2
    if (grads == null) {
      grads = new Array[Float](update.length)
      inputUpdates.put(nodeId, (inputUpdates.get(nodeId)._1, grads))
      inputUpdateCounter.put(nodeId, 0)
    }
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }

  def merge(inputUpdateCounter: Int2IntOpenHashMap, inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
            nodeId: Int, g: Float, update: Array[Float]): Int = {
    var grads: Array[Float] = inputUpdates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](update.length)
      inputUpdates.put(nodeId, grads)
      inputUpdateCounter.put(nodeId, 0)
    }
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }

  def merge(inputUpdateCounter: Object2IntOpenHashMap[String], inputUpdates: Object2ObjectOpenHashMap[String, Array[Float]],
            nodeId: String, g: Float, update: Array[Float]): Int = {
    var grads: Array[Float] = inputUpdates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](update.length)
      inputUpdates.put(nodeId, grads)
      inputUpdateCounter.put(nodeId, 0)
    }
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }

  def parseBatchData(sentences: Array[Array[Int]], windowSize: Int): (Array[Int], Array[Int]) = {
    val srcNodes = new ArrayBuffer[Int]()
    val dstNodes = new ArrayBuffer[Int]()
    for (s <- sentences.indices) {
      val sen = sentences(s)
      for (srcIndex <- sen.indices) {
        var dstIndex = Math.max(srcIndex - windowSize, 0)
        while (dstIndex < Math.min(srcIndex + windowSize + 1, sen.length)) {
          if (srcIndex != dstIndex) {
            srcNodes.append(sen(dstIndex))
            dstNodes.append(sen(srcIndex))
          }
          dstIndex += 1
        }
      }
    }
    (srcNodes.toArray, dstNodes.toArray)
  }

  def summarizeReduceOp(t1: (Int, Int, Int, Long, Long),
                        t2: (Int, Int, Int, Long, Long)): (Int, Int, Int, Long, Long) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2), math.max(t1._3, t2._3) ,t1._4 + t2._4, t1._5 + t2._5)

  def summarizeApplyOp(iterator: Iterator[Array[Int]]): Iterator[(Int, Int, Int, Long, Long)] = {
    var minId = Int.MaxValue
    var maxId = Int.MinValue
    var numDocs = 0L
    var numTokens = 0L
    var maxLength = Int.MinValue
    while (iterator.hasNext) {
      val entry = iterator.next()
      minId = math.min(minId, entry.min)
      maxId = math.max(maxId, entry.max)
      maxLength = math.max(maxLength, entry.length)
      numDocs += 1
      numTokens += entry.length
    }
    Iterator.single((minId, maxId, maxLength, numDocs, numTokens))
  }

  def getMinMaxID(inputDictRDD: RDD[String]): (Int, Int) ={
    val conf = SparkContext.getOrCreate().getConf
    val keyValueSep = conf.get("spark.hadoop.angel.line.keyvalue.sep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "\\|"
    }
    val temp = inputDictRDD.map {line =>
      val splits = line.split(keyValueSep)
      splits(0).toInt
    }
    (temp.max(), temp.min())
  }

}