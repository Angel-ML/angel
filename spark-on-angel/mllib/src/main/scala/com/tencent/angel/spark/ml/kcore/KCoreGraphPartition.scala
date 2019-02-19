package com.tencent.angel.spark.ml.kcore

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.util.collection.SparkCollectionProxy
import org.slf4j.LoggerFactory

import com.tencent.angel.ml.math2.vector.IntIntVector

private class KCoreGraphPartition(val keys: Array[Int], val adjs: Array[Array[Int]]) extends Serializable {
  assert(keys.length == adjs.length)
  private val nodes: Array[Int] = adjs.flatten.union(keys).distinct
  private val LOG = LoggerFactory.getLogger(this.getClass)

  private val invertAdjs = {
    val tempMap = SparkCollectionProxy.createOpenHashMap[Int, ArrayBuffer[Int]]()
    adjs.zipWithIndex.foreach { case (adj, index) =>
      adj.foreach { nei =>
        tempMap.changeValue(nei, new ArrayBuffer[Int]() += index, _ += index)
      }
    }
    tempMap
  }

  /*
  max id
   */
  def max: Int = keys.aggregate(Int.MinValue)(math.max, math.max)

  /*
  sum of cores
   */
  def sum(model: KCorePSModel): Double = {
    model.pull(getKeysCopy).get(keys).map(Coder.decodeCoreNumber).sum
  }

  private def getKeysCopy: Array[Int] = {
    val keyCopy = new Array[Int](keys.length)
    System.arraycopy(keys, 0, keyCopy, 0, keys.length)
    keyCopy
  }

  /*
  reset version
  we have only 6 bit to encode version, when version goes to 127, reset it to 1
   */
  def resetVersion(model: KCorePSModel): Unit = {
    import collection.JavaConversions._
    val coreWithVersions = model.pull(getKeysCopy)
    val withNewVersionFunc = Coder.withNewVersion(1)
    coreWithVersions.getStorage.entryIterator.foreach { entry =>
      val coreWithVersion = entry.getIntValue
      if (Coder.isMaxVersion(Coder.decodeVersion(coreWithVersion))) {
        entry.setValue(withNewVersionFunc(coreWithVersion))
      } else {
        entry.setValue(Coder.decodeCoreNumber(coreWithVersion))
      }
    }
    model.updateCoreWithActive(coreWithVersions)
  }

  def init(model: KCorePSModel): Unit = {
    val withVersionFunc = Coder.withVersion(1)
    val coresWithVersion = adjs.map{adj => withVersionFunc(adj.length)}
    model.updateCoreWithActive(keys, coresWithVersion)
  }

  def process(model: KCorePSModel, version: Int, enable: Boolean = false): Int = {
    val curTime = System.currentTimeMillis()
    val curCoresWithVersion = model.pull(nodes)
    LOG.info(s"[pull]${nodes.length} nodes, takes ${System.currentTimeMillis() - curTime}ms")

    // h-index
    val curTime2 = System.currentTimeMillis()
    val indices = new ArrayBuffer[Int]()
    val newEstimations = new ArrayBuffer[Int]()
    for (i <- getActive(curCoresWithVersion, enable, version)) {
      val newEst = KCoreGraphPartition.hIndex(adjs(i), curCoresWithVersion)
      if (Coder.decodeCoreNumber(curCoresWithVersion.get(keys(i))) > newEst) {
        indices += keys(i)
        newEstimations += newEst
      }
    }
    LOG.info(s"[estimate cores]${indices.length} update, takes ${System.currentTimeMillis() - curTime2}ms")

    // update
    val curTime3 = System.currentTimeMillis()
    val updateKey = indices.toArray
    val withVersionFunc = Coder.withVersion(version + 1)
    model.updateCoreWithActive(updateKey, newEstimations.map(withVersionFunc)(collection.breakOut))
    LOG.info(s"[update]takes ${System.currentTimeMillis() - curTime3}ms")
    LOG.info(s"total = ${System.currentTimeMillis() - curTime}ms")
    indices.length
  }

  private def getActive(curCores: IntIntVector, enable: Boolean, version: Int) = {
    val curTime = System.currentTimeMillis()
    val active = if (enable) {
      val activeNodes = curCores.getStorage.entryIterator().flatMap { entry =>
        if (Coder.decodeVersion(entry.getIntValue) >= version) {
          Iterator.single(entry.getIntKey)
        } else {
          Iterator.empty
        }
      }
      val activeKeys = activeNodes.flatMap { nei =>
        if (invertAdjs.contains(nei)) {
          invertAdjs(nei)
        } else {
          Iterator.empty
        }
      }.toIndexedSeq
      activeKeys.distinct
    } else {
      keys.indices
    }
    LOG.info(s"${active.size} active, takes ${System.currentTimeMillis() - curTime}ms")
    active
  }

  def save(model: KCorePSModel): Array[(Int, Int)] = {
    val keyCopy = new Array[Int](keys.length)
    System.arraycopy(keys, 0, keyCopy, 0, keys.length)
    keys.zip(model.pull(keyCopy).get(keys)).map{ case (id, coreWithVersion) =>
      (id, Coder.decodeVersion(coreWithVersion))
    }
  }
}

private[kcore] object KCoreGraphPartition {


  def fromGroup(iter: Iterator[(Int, Iterable[Int])]): KCoreGraphPartition = {
    val keys = new ArrayBuffer[Int]()
    val adjs = new ArrayBuffer[Array[Int]]()
    iter.foreach { case (key, group) =>
      keys += key
      adjs += group.toSet.toArray
    }
    new KCoreGraphPartition(keys.toArray, adjs.toArray)
  }

  // todo: to be improved
  private def hIndex(nei: Array[Int], vector: IntIntVector): Int = {
    val map = new mutable.HashMap[Int, Int]()
    vector.get(nei).foreach { core =>
      val c = Coder.decodeCoreNumber(core)
      map(c) = map.getOrElse(c, 0) + 1
    }
    val pairs = map.toArray.sortBy(_._1)
    var s = 0
    var i = pairs.length - 1
    while (i >= 0 && {
      s += pairs(i)._2
      pairs(i)._1 > s
    }) {
      i -= 1
    }
    if (i < 0) s else math.max(s - pairs(i)._2, pairs(i)._1)
  }
}
