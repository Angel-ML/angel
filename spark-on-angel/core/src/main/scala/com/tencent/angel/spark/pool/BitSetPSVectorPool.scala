package com.tencent.angel.spark.pool

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import sun.misc.Cleaner

import com.tencent.angel.spark.{PSClient, PSVectorPool, PSVectorProxy}

/**
 * BitSetPSVectorPool is an implement of [[PSVectorPool]].
 * BitSetPSVectorPool record the using vector and used vector in `bitSet`, when there is no
 * enough space for new vectors, it will release the used Vector by triggering System.gc.
 */
class BitSetPSVectorPool(
    _client: PSClient,
    _id: Int,
    _numDimensions: Int,
    _capacity: Int)
  extends PSVectorPool(_client, _id, _numDimensions, _capacity) {

  private var size = 0

  private var destroyed = false

  private val bitSet = new java.util.BitSet(capacity)

  private val cleaners = new java.util.WeakHashMap[PSVectorProxy, Cleaner]

  private class CleanTask(index: Int) extends Runnable {
    def run(): Unit = {
      bitSet.synchronized {
        bitSet.clear(index)
        size -= 1
      }
    }
  }

  /**
   * Allocate a space for a new vector.
   * It will trigger System.gc to collect the used space when
   * the available pool space is limited
   */
  private[spark] def allocate(): PSVectorProxy = {
    if (destroyed) {
      throw new SparkException("This vector pool has been destroyed!")
    }

    bitSet.synchronized {
      if (size < capacity) {
        if (size > math.max(capacity * 0.9, 4)) {
          System.gc()
        }
        val index = bitSet.nextClearBit(0)
        bitSet.set(index)
        size += 1
        return allocate(index)
      }
    }

    System.gc()
    Thread.sleep(100L)

    bitSet.synchronized {
      if (size < capacity) {
        val index = bitSet.nextClearBit(0)
        bitSet.set(index)
        size += 1
        return allocate(index)
      } else {
        throw new SparkException("This vector pool is full!")
      }
    }
  }

  private def allocate(index: Int): PSVectorProxy = {
    val task = new CleanTask(index)
    val key = new PSVectorProxy(this, id, index, numDimensions)
    cleaners.put(key, Cleaner.create(key, task))
    key
  }

  private[spark] def delete(key: PSVectorProxy): Unit = {
    cleaners.remove(key).clean()
  }

  private[spark] def destroy(): Unit = {
    destroyed = true
    cleaners.asScala.keys.toArray.foreach(_.delete())
  }

}
