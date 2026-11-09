package com.tencent.angel.spark.ml.graph.algo.embedding.node2vec

import java.util.Random
import java.util.concurrent.Future

import com.tencent.angel.graph.client.node2vec.getfuncs.getprogress.{GetProgress, GetProgressResult}
import com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor.{PullNeighbor, PullNeighborNumPart, PullNeighborNumPartParam, PullNeighborParam, PullNeighborResult, PullNumPart, PullNumPartResult}
import com.tencent.angel.graph.client.node2vec.getfuncs.pullpathtail.{PullPathTail, PullPathTailParam, PullPathTailResult}
import com.tencent.angel.graph.client.node2vec.updatefuncs.pushpathtail.{PushPathTail, PushPathTailParam}
import com.tencent.angel.ml.matrix.psf.get.base.{GetParam, GetResult}
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2IntOpenHashMap, Long2LongOpenHashMap, Long2ObjectOpenHashMap, LongOpenHashSet}

import scala.collection.mutable.{ArrayBuffer}

abstract class WalkerBase(neighbor: PSMatrix, walkPath: PSMatrix, workPartId: Int, batchSize: Int,
                          p: Double, q: Double, threshold: Int) extends Iterator[Unit] {
  private var _hasNext = true
  private var count: Long = 0
  private var pathTail: Long2ObjectOpenHashMap[Array[Long]] = _
  private val pullPathTail = new PullPathTail(new PullPathTailParam(walkPath.id, workPartId, batchSize))
  private var pullFuture: Future[GetResult] = _

  private def getProgress: (Boolean, Double) = {
    val func = new GetProgress(new GetParam(walkPath.id))
    val result = walkPath.psfGet(func).asInstanceOf[GetProgressResult]

    result.isFinished -> result.getPrecent
  }

  protected def pullNeighbor(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    neighbor.psfGet(new PullNeighbor(new PullNeighborParam(neighbor.id, nodeIds)))
      .asInstanceOf[PullNeighborResult].getResult
  }

  protected def pullNumPart(nodeIds: Array[Long]): Long2IntOpenHashMap = {
    neighbor.psfGet(new PullNumPart(new PullNeighborParam(neighbor.id, nodeIds)))
      .asInstanceOf[PullNumPartResult].getResult // Long2IntOpenHashMap
  }

  protected def pullNeighborNumPart(nodeIds: Array[Long], numParts: Long2IntOpenHashMap): Long2ObjectOpenHashMap[Array[Long]] = {//传入keyIds+numParts,保证二者一一对应。
    //分numParts情况，分次拉取，每次拉去的keyIds数递减。
    val numPartMax = numParts.values().toIntArray.max //取最大numPart数
    var result = new Long2ObjectOpenHashMap[Array[Long]]
    if (numPartMax <= 1) { // 所以keyIds对应的邻居集均不需要分part
      /*
      val partParam = new PullNeighborNumPartParam(neighbor.id, nodeIds, 0)
      val paramNumPart = new PullNeighborNumPart(partParam)
      val psfGetStep = neighbor.psfGet(paramNumPart)
      val getResults = psfGetStep.asInstanceOf[PullNeighborResult]
      result = getResults.getResult
      */
      //result = neighbor.psfGet(paramNumPart).asInstanceOf[PullNeighborResult].getResult

      result = neighbor.psfGet(new PullNeighborNumPart(new PullNeighborNumPartParam(neighbor.id, nodeIds, 0)))
        .asInstanceOf[PullNeighborResult].getResult

    } else {
      result = neighbor.psfGet(new PullNeighborNumPart(new PullNeighborNumPartParam(neighbor.id, nodeIds,0)))
        .asInstanceOf[PullNeighborResult].getResult // 取所有keyIds邻居集的第一part
      for (numPart <- 1 until numPartMax) {
        val newNodeIds = getKeyIdsByNumPart(numParts, numPart+1).sorted//选出满足value大于等于(numPart+1)的keyIds
        val resultPart = neighbor.psfGet(new PullNeighborNumPart(new PullNeighborNumPartParam(neighbor.id, newNodeIds, numPart)))
          .asInstanceOf[PullNeighborResult].getResult
        result = mergeResultPart(result, resultPart)
      }
    }
    result
  }

  private def getKeyIdsByNumPart(numParts: Long2IntOpenHashMap, numPart: Int): Array[Long] = {
    val keyIds = new ArrayBuffer[Long]()
    val keyNumParts = numParts.keySet.toLongArray.sorted
    //println("keyNumParts is null or not: " + (keyNumParts == null))
    for (i <- keyNumParts) {// keySet保序吗？
      //println("key is in numParts or not: " + numParts.containsKey(i))
      if (numParts.get(i) >= numPart) {//nullPointer?
        //println("get numParts that are bigger than numPart successfully: " + numParts.get(i))
        keyIds.append(i)
      }
    }
    keyIds.toArray
  }

  private def mergeResultPart(result: Long2ObjectOpenHashMap[Array[Long]],
                              resultPart: Long2ObjectOpenHashMap[Array[Long]]): Long2ObjectOpenHashMap[Array[Long]] = {
    for (i <- resultPart.keySet.toLongArray.sorted) {
      result.put(i, result.get(i).clone()++resultPart.get(i).clone())
    }
    result
  }

  override def hasNext: Boolean = _hasNext

  override def next(): Unit = {
    pathTail = if (pullFuture == null) {
      pullFuture = walkPath.asyncPsfGet(pullPathTail)
      pullFuture.get().asInstanceOf[PullPathTailResult].getResult
    } else {
      pullFuture.get().asInstanceOf[PullPathTailResult].getResult
    }
    pullFuture = walkPath.asyncPsfGet(pullPathTail)

    if (pathTail != null && pathTail.size() > 0) { // get data
      val newPathTail = compute(pathTail)
      count += pathTail.size()
      val func = new PushPathTail(new PushPathTailParam(walkPath.id, newPathTail))
      walkPath.asyncPsfUpdate(func)
    } else { // don't get data
      val (isFinished, percent) = getProgress
      if (isFinished) {
        println(s"${Thread.currentThread().getId} $count times sample finished!")
        println(s"${Thread.currentThread().getId} the global progress is $percent")
        _hasNext = false
      }
    }
  }

  def nextV2(): Unit = {
    pathTail = if (pullFuture == null) {
      pullFuture = walkPath.asyncPsfGet(pullPathTail)
      pullFuture.get().asInstanceOf[PullPathTailResult].getResult
    } else {
      pullFuture.get().asInstanceOf[PullPathTailResult].getResult
    }
    pullFuture = walkPath.asyncPsfGet(pullPathTail)

    if (pathTail != null && pathTail.size() > 0) { // get data
      val newPathTail = computeV2(pathTail)
      count += pathTail.size()
      val func = new PushPathTail(new PushPathTailParam(walkPath.id, newPathTail))
      walkPath.asyncPsfUpdate(func)
    } else { // don't get data
      val (isFinished, percent) = getProgress
      if (isFinished) {
        println(s"${Thread.currentThread().getId} $count times sample finished!")
        println(s"${Thread.currentThread().getId} the global progress is $percent")
        _hasNext = false
      }
    }
  }

  protected def compute(pathTail: Long2ObjectOpenHashMap[Array[Long]]): Long2LongOpenHashMap

  protected def computeV2(pathTail: Long2ObjectOpenHashMap[Array[Long]]): Long2LongOpenHashMap
}

class RejectSamplingWalker(neighbor: PSMatrix, walkPath: PSMatrix, workPartId: Int, batchSize: Int,
                           p: Double, q: Double, threshold: Int, useCache: Boolean, cacheInitCapacity: Int)
  extends WalkerBase(neighbor, walkPath, workPartId, batchSize, p, q, threshold) {

  private val cachedNeigh: CachedNeigh = if (useCache) {
    CachedNeigh.init(cacheInitCapacity)
  } else {
    null.asInstanceOf[CachedNeigh]
  }

  private val upperBound: Double = Math.max(1.0, Math.max(1 / p, 1 / q))
  private val lowerBound: Double = Math.min(1.0, Math.min(1 / p, 1 / q))

  private implicit val rand: Random = new Random()

  private def rejectSampling(pathTail: Long2ObjectOpenHashMap[Array[Long]],
                             neighs: Long2ObjectOpenHashMap[LongOpenHashSet],
                             flags: Long2DoubleOpenHashMap): Long2LongOpenHashMap = {
    val newPathTail = new Long2LongOpenHashMap(pathTail.size())
    val iter = pathTail.long2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val tag = entry.getLongKey
      val Array(src, dst) = entry.getValue

      val dstNeigh = neighs.get(dst)

      var choosed: Long = choice(dstNeigh)// pick up one neighbor randomly
      var ai = flags.get(tag)
      // if ai < lowerBound, then accept
      if (ai >= lowerBound) { // try more
        val srcNeigh = neighs.get(src)

        var isAccept: Boolean = false
        while (!isAccept) {
          isAccept = if (srcNeigh.contains(choosed) && ai < 1.0) { // accept
            true
          } else if (src == choosed && ai < 1 / p) { // accept
            true
          } else if (ai < 1 / q) { // accept
            true
          } else { // reject
            false
          }

          choosed = choice(dstNeigh)
          ai = uniform(0.0, upperBound)
        }
      }

      newPathTail.put(tag, choosed)
    }

    newPathTail
  }

  private def choice(neigh: LongOpenHashSet): Long = {
    if (!neigh.isEmpty) {
      val iter = neigh.iterator
      if (neigh.size == 1) {
        iter.hasNext
        iter.nextLong()
      } else {
        iter.skip(rand.nextInt(neigh.size))
        if (iter.hasNext) {
          iter.next()
        } else {
          throw new Exception("Error, please check the dstNeigh !")
        }
      }
    } else {
      throw new Exception("is put is empty!")
    }
  }

  private def uniform(lower: Double, upper: Double): Double = {
    val range = upper - lower
    rand.nextDouble() * range + lower
  }

  protected def compute(pathTail: Long2ObjectOpenHashMap[Array[Long]]): Long2LongOpenHashMap = {
    val iter = pathTail.long2ObjectEntrySet().fastIterator()
    val maybeNeedPullKeyBuf = new LongOpenHashSet(2 * pathTail.size())
    val flags = new Long2DoubleOpenHashMap(pathTail.size())

    while (iter.hasNext) {
      val entry = iter.next()
      val tag = entry.getLongKey
      val Array(src, dst) = entry.getValue

      val ai = uniform(0.0, upperBound)
      flags.put(tag, ai)
      if (ai > lowerBound) {
        // reject at the first, so only the neighbors of src are required
        maybeNeedPullKeyBuf.add(src)
      }

      maybeNeedPullKeyBuf.add(dst)
    }

    val neighs = if (useCache) {
      val needPullKeys = cachedNeigh.getUnCached(maybeNeedPullKeyBuf)
      val pulledNeigh = if (needPullKeys.nonEmpty) {
        println(s"${Thread.currentThread()}, number of neighs need pull ${needPullKeys.length}")
        val pulled = pullNeighbor(needPullKeys)
        val needCache = new Long2ObjectOpenHashMap[LongOpenHashSet](pulled.size())

        val iter = pulled.long2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val key = entry.getLongKey
          val value = entry.getValue

          if (value.length >= threshold) {
            needCache.put(key, CachedNeigh.arr2set(value))
          }
        }

        if (!needCache.isEmpty) {
          cachedNeigh.syncPut(needCache)
        }
        pulled
      } else {
        new Long2ObjectOpenHashMap[Array[Long]]()
      }
      cachedNeigh.getOrDefault(maybeNeedPullKeyBuf, pulledNeigh)
    } else {
      if (!maybeNeedPullKeyBuf.isEmpty) {
        val maybeNeedPullKeys = CachedNeigh.set2arr(maybeNeedPullKeyBuf)
        println(s"${Thread.currentThread()}, number of neighs need pull ${maybeNeedPullKeys.length}")
        val pulled = pullNeighbor(maybeNeedPullKeys)
          .asInstanceOf[Long2ObjectOpenHashMap[Array[Long]]]

        val neighs = new Long2ObjectOpenHashMap[LongOpenHashSet](pulled.size())
        val iter = pulled.long2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val value = entry.getValue
          val set = new LongOpenHashSet(value.length)
          value.foreach(x => set.add(x))
          neighs.put(entry.getLongKey, set)
        }

        neighs
      } else {
        new Long2ObjectOpenHashMap[LongOpenHashSet]()
      }
    }

    rejectSampling(pathTail, neighs, flags)//lower bound dynamic random walk implemention
  }

  protected def computeV2(pathTail: Long2ObjectOpenHashMap[Array[Long]]): Long2LongOpenHashMap = {
    val iter = pathTail.long2ObjectEntrySet().fastIterator()
    val maybeNeedPullKeyBuf = new LongOpenHashSet(2 * pathTail.size())
    val flags = new Long2DoubleOpenHashMap(pathTail.size())

    while (iter.hasNext) {
      val entry = iter.next()
      val tag = entry.getLongKey
      val Array(src, dst) = entry.getValue

      val ai = uniform(0.0, upperBound)
      flags.put(tag, ai)
      if (ai > lowerBound) {
        // reject at the first, so only the neighbors of src are required
        maybeNeedPullKeyBuf.add(src)
      }

      maybeNeedPullKeyBuf.add(dst)
    }
    val neighs = if (useCache) {
      val needPullKeys = cachedNeigh.getUnCached(maybeNeedPullKeyBuf)
      val pulledNeigh = if (needPullKeys.nonEmpty) {
        println(s"${Thread.currentThread()}, number of neighs need pull ${needPullKeys.length}")
        needPullKeys.sorted
        val numParts = pullNumPart(needPullKeys)
        val pulled = pullNeighborNumPart(needPullKeys, numParts)
        val needCache = new Long2ObjectOpenHashMap[LongOpenHashSet](pulled.size())

        val iter = pulled.long2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val key = entry.getLongKey
          val value = entry.getValue

          if (value.length >= threshold) {
            needCache.put(key, CachedNeigh.arr2set(value))
          }
        }

        if (!needCache.isEmpty) {
          cachedNeigh.syncPut(needCache)
        }
        pulled
      } else {
        new Long2ObjectOpenHashMap[Array[Long]]()
      }
      cachedNeigh.getOrDefault(maybeNeedPullKeyBuf, pulledNeigh)
    } else {
      if (!maybeNeedPullKeyBuf.isEmpty) {
        val maybeNeedPullKeys = CachedNeigh.set2arr(maybeNeedPullKeyBuf)
        maybeNeedPullKeys.sorted
        println(s"${Thread.currentThread()}, number of neighs need pull without cache ${maybeNeedPullKeys.length}")
        val numParts = pullNumPart(maybeNeedPullKeys)
        val pulled = pullNeighborNumPart(maybeNeedPullKeys, numParts)
        val neighs = new Long2ObjectOpenHashMap[LongOpenHashSet](pulled.size())
        val iter = pulled.long2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val value = entry.getValue
          val set = new LongOpenHashSet(value.length)
          value.foreach(x => set.add(x))
          neighs.put(entry.getLongKey, set)
        }

        neighs
      } else {
        new Long2ObjectOpenHashMap[LongOpenHashSet]()
      }
    }

    rejectSampling(pathTail, neighs, flags)
  }
}
