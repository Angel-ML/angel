package com.tencent.angel.spark.ml.graph.node2vec

import java.util.Random

import com.tencent.angel.graph.client.node2vec.PartitionHasher
import com.tencent.angel.graph.client.node2vec.getfuncs.getprogress.{GetProgress, GetProgressResult}
import com.tencent.angel.ml.matrix.psf.get.base.GetParam
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap, Long2ObjectOpenHashMap}

import scala.collection.mutable
import scala.reflect.ClassTag

abstract class WalkerBase(override protected val neighbor: PSMatrix,
                          protected val walkPath: PSMatrix,
                          protected val puller: TailPuller,
                          protected val pusher: TailPusher,
                          protected val p: Double,
                          protected val q: Double,
                          protected val degreeBinSize: Int,
                          protected val hitRatio: Double) extends Iterator[Unit] with NeighborUtils {
  private var _hasNext = true

  protected lazy val threshold: Int = getDegreeThreshold(degreeBinSize, hitRatio)

  private def getProgress: (Boolean, Double) = {
    val func = new GetProgress(new GetParam(walkPath.id))
    val result = walkPath.psfGet(func).asInstanceOf[GetProgressResult]

    result.isFinished -> result.getPrecent
  }

  override def hasNext: Boolean = _hasNext

  override def next(): Unit = {
    val pathTail = puller.pull()
    if (pathTail.size() == 0) {
      val (isFinished, progress) = getProgress
      println(s"the progress is $progress")

      if (isFinished) {
        puller.shutDown()
        pusher.shutDown()
      }
      _hasNext = !isFinished
    } else {
      val newPathTail = compute(pathTail)
      pusher.push(newPathTail)
    }
  }

  protected def compute(pathTail: Long2ObjectOpenHashMap[Array[Long]]): Long2LongOpenHashMap

}


class AliasWalker[ED: ClassTag](aliasTable: Map[(Long, Long), (Array[Long], Array[Long], Array[Long])],
                                neighbor: PSMatrix, walkPath: PSMatrix,
                                puller: TailPuller, pusher: TailPusher, p: Double, q: Double)
  extends WalkerBase(neighbor, walkPath, puller, pusher, p, q, -1, 0.0) {

  private def aliasSample(src: Long, com: Array[Long], disSide: Array[Long]): Long = {
    val tol = 1.0 / p + com.length + disSide.length / q

    val rand = new Random
    val pivot = rand.nextDouble()

    if (pivot < 1.0 / p / tol) {
      src
    } else if (pivot < (1.0 / p + com.length) / tol) {
      com(rand.nextInt(com.length))
    } else {
      disSide(rand.nextInt(disSide.length))
    }
  }

  override protected def compute(pathTail: Long2ObjectOpenHashMap[Array[Long]]): Long2LongOpenHashMap = {
    val newPathTail = new Long2LongOpenHashMap(pathTail.size())

    val tailIter = pathTail.entrySet().iterator()
    while (tailIter.hasNext) {
      val entry = tailIter.next()
      val tag = entry.getKey.asInstanceOf[Long]
      val Array(src, dst) = entry.getValue

      // println(s"$src -> $dst")
      val nextStep = if (src < dst) {
        try {
          val (_, com, dstSide) = aliasTable(src -> dst)
          aliasSample(src, com, dstSide)
        } catch {
          case e: NoSuchElementException =>
            e.printStackTrace()
            println((src, dst) + ": " + PartitionHasher.getHash(src, dst, 4))
            src
        }
      } else {
        try {
          val (dstSide, com, _) = aliasTable(dst -> src)
          aliasSample(src, com, dstSide)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            println((src, dst) + ": " + PartitionHasher.getHash(src, dst, 4))
            src
        }
      }

      newPathTail.put(tag, nextStep)
    }

    newPathTail
  }
}


class JITAliasWalker(neighbor: PSMatrix, walkPath: PSMatrix, puller: TailPuller, pusher: TailPusher,
                     p: Double, q: Double, degreeBinSize: Int, hitRatio: Double)
  extends WalkerBase(neighbor, walkPath, puller, pusher, p, q, degreeBinSize, hitRatio) {
  private val cachedNeigh = new Long2ObjectOpenHashMap[Set[Long]]()

  private def aliasSample(pathTail: Long2ObjectOpenHashMap[Array[Long]],
                          left: Long2ObjectOpenHashMap[Set[Long]]): Long2LongOpenHashMap = {
    val newPathTail = new Long2LongOpenHashMap(pathTail.size())
    val iter = pathTail.long2ObjectEntrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val tag = entry.getLongKey
      val Array(src, dst) = entry.getValue

      val srcNeigh = if (cachedNeigh.containsKey(src)) {
        cachedNeigh.get(src)
      } else if (left.containsKey(src)) {
        left.get(src)
      } else {
        throw new Exception(s"$src not found in both cachedNeigh and left")
      }

      val dstNeigh = if (cachedNeigh.containsKey(dst)) {
        cachedNeigh.get(dst)
      } else if (left.containsKey(dst)) {
        left.get(dst)
      } else {
        throw new Exception(s"$dst not found in both cachedNeigh and left")
      }

      val com = srcNeigh & dstNeigh
      val dstSide = (dstNeigh -- com).toArray

      val tol = 1.0 / p + com.size + dstSide.length / q

      val rand = new Random
      val pivot = rand.nextDouble()

      val sampled = if (pivot < 1.0 / p / tol) {
        src
      } else if (pivot < (1.0 / p + com.size) / tol) {
        val comArray = com.toArray
        comArray(rand.nextInt(com.size))
      } else {
        dstSide(rand.nextInt(dstSide.length))
      }

      newPathTail.put(tag, sampled)
    }

    newPathTail
  }

  override protected def compute(pathTail: Long2ObjectOpenHashMap[Array[Long]]): Long2LongOpenHashMap = {
    val tailValueIter = pathTail.values().iterator()
    val buf = new mutable.HashSet[Long]()
    while (tailValueIter.hasNext) {
      val Array(src, dst) = tailValueIter.next()

      if (!cachedNeigh.containsKey(src)) {
        buf.add(src)
      }

      if (!cachedNeigh.containsKey(dst)) {
        buf.add(dst)
      }
    }

    val left = if (buf.nonEmpty) {
      val pulled = pullNeighborTable[Long](buf.toArray)
        .asInstanceOf[Long2ObjectOpenHashMap[Array[Long]]]

      val remain = new Long2ObjectOpenHashMap[Set[Long]](pulled.size())
      val iter = pulled.long2ObjectEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getLongKey
        val value = entry.getValue

        if (value.length >= threshold) {
          cachedNeigh.put(key, value.toSet)
        } else {
          remain.put(key, value.toSet)
        }
      }

      remain
    } else {
      new Long2ObjectOpenHashMap[Set[Long]]()
    }

    buf.clear()
    aliasSample(pathTail, left)
  }
}


class RejectSamplingWalker(neighbor: PSMatrix, walkPath: PSMatrix, puller: TailPuller, pusher: TailPusher,
                           p: Double, q: Double, degreeBinSize: Int, hitRatio: Double)
  extends WalkerBase(neighbor, walkPath, puller, pusher, p, q, degreeBinSize, hitRatio) {
  private val cachedNeigh = new Long2ObjectOpenHashMap[Set[Long]]()

  private val upperBound: Double = Math.max(1.0, Math.max(1 / p, 1 / q))
  private val lowerBound: Double = Math.min(1.0, Math.min(1 / p, 1 / q))

  private implicit val rand: Random = new Random()

  private def rejectSampling(pathTail: Long2ObjectOpenHashMap[Array[Long]],
                             left: Long2ObjectOpenHashMap[Set[Long]],
                             flags: Long2DoubleOpenHashMap): Long2LongOpenHashMap = {
    val newPathTail = new Long2LongOpenHashMap(pathTail.size())
    val iter = pathTail.long2ObjectEntrySet().fastIterator()

    while (iter.hasNext) {
      val entry = iter.next()
      val tag = entry.getLongKey
      val Array(src, dst) = entry.getValue

      val dstNeigh = if (cachedNeigh.containsKey(dst)) {
        cachedNeigh.get(dst)
      } else if (left.containsKey(dst)) {
        left.get(dst)
      } else {
        throw new Exception(s"$dst not found in both cachedNeigh and left")
      }

      var choosed: Long = choice(dstNeigh)
      var ai = flags.get(tag)
      // if ai < lowerBound, then accept
      if (ai >= lowerBound) { // try more
        val srcNeigh = if (cachedNeigh.containsKey(src)) {
          cachedNeigh.get(src)
        } else if (left.containsKey(src)) {
          left.get(src)
        } else {
          throw new Exception(s"$src not found in both cachedNeigh and left")
        }

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

  private def choice(neigh: Set[Long]): Long = {
    if (neigh.nonEmpty) {
      if (neigh.size == 1) {
        neigh.head
      } else {
        val iter = neigh.iterator
        val idx = rand.nextInt(neigh.size)
        var i = 0
        while (i < idx && iter.hasNext) {
          iter.next()
          i += 1
        }
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

  override protected def compute(pathTail: Long2ObjectOpenHashMap[Array[Long]]): Long2LongOpenHashMap = {
    val iter = pathTail.long2ObjectEntrySet().fastIterator()
    val buf = new mutable.HashSet[Long]()
    val flags = new Long2DoubleOpenHashMap(pathTail.size())

    while (iter.hasNext) {
      val entry = iter.next()
      val tag = entry.getLongKey
      val Array(src, dst) = entry.getValue

      val ai = uniform(0.0, upperBound)
      flags.put(tag, ai)
      if (ai > lowerBound) {
        // reject at the first, so only the neighbors of src are required
        if (!cachedNeigh.containsKey(src)) {
          buf.add(src)
        }
      }

      // on any condition, the neighbors of dst are required
      if (!cachedNeigh.containsKey(dst)) {
        buf.add(dst)
      }
    }

    val left = if (buf.nonEmpty) {
      val pulled = pullNeighborTable[Long](buf.toArray)
        .asInstanceOf[Long2ObjectOpenHashMap[Array[Long]]]

      val remain = new Long2ObjectOpenHashMap[Set[Long]](pulled.size())
      val iter = pulled.long2ObjectEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getLongKey
        val value = entry.getValue

        if (value.length >= threshold) {
          cachedNeigh.put(key, value.toSet)
        } else {
          remain.put(key, value.toSet)
        }
      }

      remain
    } else {
      new Long2ObjectOpenHashMap[Set[Long]]()
    }

    buf.clear()
    rejectSampling(pathTail, left, flags)
  }
}