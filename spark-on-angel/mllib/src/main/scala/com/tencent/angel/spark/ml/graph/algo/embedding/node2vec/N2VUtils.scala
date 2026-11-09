package com.tencent.angel.spark.ml.graph.algo.embedding.node2vec

import java.util

import com.tencent.angel.graph.client.node2vec.getfuncs.getmaxdegree.{PullMaxDegree, PullMaxDegreeResult}
import com.tencent.angel.graph.client.node2vec.getfuncs.getmindegree.{PullMinDegree, PullMinDegreeResult}
import com.tencent.angel.graph.client.node2vec.getfuncs.getstats.{GetStats, GetStatsParam, GetStatsResult}
import com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor.{PullNeighbor, PullNeighborParam, PullNeighborResult}
import com.tencent.angel.graph.client.sampleneighbor.{SampleNeighbor, SampleNeighborParam, SampleNeighborResult}
import com.tencent.angel.ml.matrix.psf.get.base.GetParam
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.data.NeighborTablePartition
import com.tencent.angel.spark.ml.graph.utils.params.HasPullSizeInMB
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.matching.Regex

trait NeighborUtils extends Serializable {
  protected val neighbor: PSMatrix

  protected def pullNeighborTable[@specialized(Int, Long) ED: ClassTag](nodeIds: Array[ED], count: Int = -1): AnyRef = {
    val tag = implicitly[ClassTag[ED]]
    tag.runtimeClass match {
      case clz if clz == classOf[Int] =>
        neighbor.psfGet(new SampleNeighbor(
          new SampleNeighborParam(neighbor.id, nodeIds.asInstanceOf[Array[Int]], count))
        ).asInstanceOf[SampleNeighborResult].getNodeIdToNeighbors
      case clz if clz == classOf[Long] =>
        neighbor.psfGet(new PullNeighbor(
          new PullNeighborParam(neighbor.id, nodeIds.asInstanceOf[Array[Long]]))
        ).asInstanceOf[PullNeighborResult].getResult
    }
  }
}

object NeighborUtils {
  private val kb: Regex = "^(\\d+)(?:k|K|kb|KB|Kb|kB)$".r
  private val mb: Regex = "^(\\d+)(?:m|M|mb|MB|Mb|mB)$".r
  private val gb: Regex = "^(\\d+)(?:g|G|gb|GM|Gb|gB)$".r
  private val num: Regex = "^(\\d+)$".r
  private val ELEMENT_SIZE: Int = 12
  private val NUM_BUCKETS: Int = 100

  /*
    val psMemory = conf.get("spark.ps.memory") match {
      case kb(size) =>
        size.toLong * 1024
      case mb(size) =>
        size.toLong * 1024 * 1024
      case gb(size) =>
        size.toLong * 1024 * 1024 * 1024
      case num(size) =>
        size.toLong
      case _ => throw new Exception("spark.ps.memory not set")
    }
    val psInstances = conf.get("spark.ps.instances").toInt
    val psCores = conf.get("spark.ps.cores").toInt
    val executorInstances = conf.get("spark.executor.instances").toInt
    val executorCores = conf.get("spark.executor.cores").toInt
  */

  case class StatsParams(minDegree: Int, maxDegreeInt: Int, threshold: Int,
                         estimatedEdge: Long, maxCachedNum: Int, numNode: Long, numEdge: Long,
                         hitRatio: Double, batchSize: Int)

  private def getStats(neighbor: PSMatrix, numBuckets: Int = 100): (Int, Int, Long, Long, Array[(Double, Double)]) = {
    val getMin = new PullMinDegree(new GetParam(neighbor.id))
    val minDegree = neighbor.psfGet(getMin).asInstanceOf[PullMinDegreeResult].getResult

    val getMax = new PullMaxDegree(new GetParam(neighbor.id))
    val maxDegree = neighbor.psfGet(getMax).asInstanceOf[PullMaxDegreeResult].getResult

    val getStats = new GetStats(new GetStatsParam(neighbor.id, maxDegree, minDegree, numBuckets))
    val statsResult = neighbor.psfGet(getStats).asInstanceOf[GetStatsResult]

    val numNode = statsResult.getNumNode
    val numEdge = statsResult.getNumEdge

    val bucket = statsResult.getBuckets
    val keys = bucket.keySet().toIntArray
    util.Arrays.sort(keys)
    // println(keys.mkString("[", ",", "]"))
    // val binLen = (maxDegree - minDegree + 1.0) / numBuckets
    println("statistics --------------------------------")
    println(s"the min degree is $minDegree")
    println(s"the max degree is $maxDegree")
    println(s"the number of node is $numNode")
    println(s"the number of edge is $numEdge, maybe doubled, pls. check!")
    println("________________")
    val tmp = Math.log(maxDegree - minDegree + 1) / numBuckets
    val hist = keys.map { key =>
      // get x and y of power law distribution buy using interpolation
      val x = Math.exp(tmp * (key + 0.5))
      val y = 1.0 * bucket.get(key)
      println(f"$x%.2f\t$y%.2f")
      x -> y
    }
    println("statistics --------------------------------")

    (minDegree, maxDegree, numNode, numEdge, hist)
  }

  def getStatsAndParams(conf: SparkConf, neighbor: PSMatrix, isTrunc: Boolean, psPartNum: Int,
                        p: Double, q: Double, cacheRatio: Double, pullSize: Int): StatsParams = {
    val (minDegree, maxDegree, numNode, numEdge, statistics) = getStats(neighbor, NUM_BUCKETS)
    val maxMemory = (Runtime.getRuntime.maxMemory() * 0.8).toLong.toString
    val executorMemory = conf.get("spark.executor.memory", maxMemory) match {
      case kb(size) =>
        size.toLong * 1024
      case mb(size) =>
        size.toLong * 1024 * 1024
      case gb(size) =>
        size.toLong * 1024 * 1024 * 1024
      case num(size) =>
        size.toLong
      case _ => throw new Exception("spark.executor.memory not set")
    }

    val maxCacheMemory = executorMemory * cacheRatio

    var sum: Double = 0.0
    var threshold = -1
    var estimatedEdge: Double = 0.0
    var maxCachedNum: Double = 0.0

    statistics.foreach { case (key: Double, count: Double) =>
      sum += key * count
    }

    val reversed = statistics.sortWith { (first: (Double, Double), second: (Double, Double)) => first._1 > second._1 }

    val hitRatio = if (isTrunc) {
      var tempSum = 0.0
      var tempHr = 0.0
      reversed.foreach { case (key: Double, count: Double) =>
        if (estimatedEdge * ELEMENT_SIZE <= maxCacheMemory) {
          tempSum += key * count
          tempHr = tempSum / sum
          threshold = key.toInt
          maxCachedNum += count
          estimatedEdge = maxCachedNum * threshold / (1.0 - tempHr)
        }
      }

      Math.min(tempHr, 1.0)
    } else {
      reversed.foreach { case (key: Double, count: Double) =>
        if (estimatedEdge * ELEMENT_SIZE <= maxCacheMemory) {
          estimatedEdge += key * count
          threshold = key.toInt
          maxCachedNum += count
        }
      }

      // calculate hitRatio
      Math.min(estimatedEdge / sum, 1.0)
    }

    val avgEdge = 1.0 * numEdge / numNode
    val min = Math.min(Math.min(1.0 / p, 1.0), 1 / q)
    val max = Math.max(Math.max(1.0 / p, 1.0), 1 / q)

    val factor = psPartNum * (max * 2 - min) / max * (1 - hitRatio / 2.0) * avgEdge * ELEMENT_SIZE / 1024 / 1024
    val batchSize = (pullSize / factor).toInt

    StatsParams(minDegree, maxDegree, threshold, estimatedEdge.toLong, maxCachedNum.toInt, numNode, numEdge, hitRatio, batchSize)
  }

  private def getThresholdByStats(statistics: Array[(Double, Double)], hr: Double): (Int, Long, Int) = {
    var sum = 0.0
    val (threshold, estimatedEdge) = statistics.map { case (x, y) =>
      sum += x * y
      x -> sum
    }.collectFirst {
      case (x, sumY) if sumY / sum >= (1 - hr) => (x.toInt, (sum - sumY).toInt)
    }.get

    val maxCachedNum = statistics.filter(p => p._1 >= threshold).map(p => p._2).sum.toInt

    println(s"the estimated edge is ${sum.toLong}, $estimatedEdge, $maxCachedNum, $threshold")

    (threshold, estimatedEdge, maxCachedNum)
  }

  private def getThresholdByCurve(statistics: Array[(Double, Double)], maxDegree: Int, hr: Double): (Int, Long, Int) = {
    var numEles = 0
    var (avgX, avgY, avgXY, avgX2, sum) = (0.0, 0.0, 0.0, 0.0, 0.0)
    statistics.foreach { case (x, y) =>
      if (y >= Math.E) {
        sum += x * y
        // y = C x^{-\alpha} -> \log{y} = \log{C} - \alpha \log{x}
        // so transform x and y to log(x) and log(y), so that log(x) is linear to log(y)
        val logX = Math.log(x)
        val logY = Math.log(y)

        // println(s"$x, $y, $logX, $logY")

        // to calculate \alpha, it's required avgX, avgY, avgXY, avgX2
        avgX += logX
        avgY += logY
        avgXY += logX * logY
        avgX2 += logX * logX

        numEles += 1
      }
    }

    avgX /= numEles
    avgY /= numEles
    avgXY /= numEles
    avgX2 /= numEles

    // calculate the alpha of power law distribution
    val alpha = -(avgXY - avgX * avgY) / (avgX2 - avgX * avgX)
    println(s"alpha is $alpha")
    val c = Math.exp(avgY - alpha * avgX)
    println(s"c is $c")

    // HR = 1 - \frac{t^{1-\alpha} - 1}{n^{2-\alpha} - 1}
    // -> t^{2-\alpha} = 1 + (1 - HR)(n^{2-\alpha} - 1)
    // -> t = \exp{(\frac{1 + (1 - HR)(n^{2-\alpha} - 1)}{2-\alpha})}
    val k = 2 - alpha
    val a = 1 + (1 - hr) * (Math.pow(maxDegree, k) - 1)
    val threshold = Math.exp(Math.log(a) / k).toInt

    val estimatedEdge = (Math.pow(maxDegree, 2 - alpha) - Math.pow(threshold, 2 - alpha)) * c / (2 - alpha)
    val maxCachedNum = statistics.filter(p => p._1 >= threshold).map(p => p._2).sum.toInt

    println(s"the estimated edge is ${sum.toLong}, $estimatedEdge, $maxCachedNum, $threshold")

    (threshold, estimatedEdge.toLong, maxCachedNum)
  }

  def getStatsThreshold(neighbor: PSMatrix, numBuckets: Int = 100, hr: Double = 0.5): (Int, Int, Long, Long, Int, Long, Int) = {
    val (minVal, maxVal, numNode, numEdge, statistics) = getStats(neighbor, numBuckets)

    val (threshold, estimatedEdge, maxCachedNum) = getThresholdByStats(statistics, hr)
    //    val (threshold2, maxCachedNum2) = getThresholdByCurve(statistics, maxVal, hr)
    //    val threshold = (threshold1 + threshold2)/2.0
    //    val maxCachedNum = (maxCachedNum1 + maxCachedNum2)/2.0

    (minVal, maxVal, numNode, numEdge, threshold.toInt, estimatedEdge, maxCachedNum.toInt)
  }
}

class AliasUtils(override protected val neighbor: PSMatrix) extends NeighborUtils with Serializable {
  type AliasTableKey = (Long, Long)
  type AliasTableValue = (Array[Long], Array[Long], Array[Long])

  def createAliasTable(neighborTable: RDD[NeighborTablePartition[Long]],
                       degreeBinSize: Int, hitRatio: Double): RDD[Map[(Long, Long), (Array[Long], Array[Long], Array[Long])]] = {
    val numPartition = neighborTable.getNumPartitions
    val (_, _, _, _, threshold, _, _) = NeighborUtils.getStatsThreshold(neighbor, degreeBinSize, hitRatio)
    val aliasUnShuffle = neighborTable.mapPartitions { partIter =>
      PSContext.instance()
      val part = partIter.next()
      val srcIds = part.srcIds
      val neighbors: Array[Set[Long]] = part.neighbors.map(_.toSet)
      val srcMap = srcIds.zipWithIndex.toMap
      val numEdge = neighbors.map(_.size).sum

      val batchSize = 1024
      val dstBuf = new ArrayBuffer[Long](batchSize)
      val keyBuf = new ArrayBuffer[AliasTableKey](batchSize)
      val aliasTable = new ArrayBuffer[(AliasTableKey, AliasTableValue)](numEdge)
      val cache = new Long2ObjectOpenHashMap[Set[Long]]()

      srcIds.zip(part.neighbors).foreach { case (src, neighbor: Array[Long]) =>
        neighbor.foreach {
          case neigh if src < neigh =>
            if (dstBuf.size == batchSize || keyBuf.size == batchSize) {
              createBatchAliasTable(srcMap, neighbors, cache, threshold, aliasTable, dstBuf, keyBuf)
            }

            if (!srcMap.contains(neigh) && !cache.containsKey(neigh)) {
              dstBuf.append(neigh)
            }
            keyBuf.append(src -> neigh)
          case _ =>
        }
      }

      if (keyBuf.nonEmpty) {
        createBatchAliasTable(srcMap, neighbors, cache, threshold, aliasTable, dstBuf, keyBuf)
      }

      cache.clear()
      aliasTable.iterator
    }

    val aliasShuffle = aliasUnShuffle.partitionBy(new AngelHashPartitioner(numPartition)).mapPartitions { iter =>
      Iterator.single(iter.toMap)
    }
    aliasShuffle.count()
    aliasShuffle
  }

  private def createBatchAliasTable(srcMap: Map[Long, Int], neighbors: Array[Set[Long]],
                                    cache: Long2ObjectOpenHashMap[Set[Long]], threshold: Int,
                                    aliasTable: ArrayBuffer[(AliasTableKey, AliasTableValue)],
                                    dstBuf: ArrayBuffer[Long], keyBuf: ArrayBuffer[AliasTableKey]): Unit = {
    val pulledNeighborTable = if (dstBuf.nonEmpty) {
      pullNeighborTable[Long](dstBuf.toArray)
        .asInstanceOf[Long2ObjectOpenHashMap[Array[Long]]]
    } else {
      null.asInstanceOf[Long2ObjectOpenHashMap[Array[Long]]]
    }

    keyBuf.foreach { case (s, d) =>
      val sn = neighbors(srcMap(s))
      val dn = if (srcMap.contains(d)) {
        neighbors(srcMap(d))
      } else {
        if (cache.containsKey(d)) {
          cache.get(d)
        } else {
          val distNeigh = pulledNeighborTable.get(d).toSet
          if (distNeigh.size >= threshold) {
            cache.put(d, distNeigh)
          }
          distNeigh
        }
      }

      val com = sn & dn
      val srcSide = sn -- com
      val dstSide = dn -- com

      aliasTable.append((s, d) -> (srcSide.toArray, com.toArray, dstSide.toArray))
    }

    dstBuf.clear()
    keyBuf.clear()
  }
}
