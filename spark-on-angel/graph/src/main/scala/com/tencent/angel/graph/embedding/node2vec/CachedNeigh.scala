package com.tencent.angel.graph.embedding.node2vec

import java.util.concurrent.locks.ReentrantReadWriteLock

import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongOpenHashSet}

import scala.collection.mutable.ListBuffer

class CachedNeigh private(capacity: Int) {
  private var cache: Long2ObjectOpenHashMap[LongOpenHashSet] =
    new Long2ObjectOpenHashMap[LongOpenHashSet](capacity)
  private val lock = new ReentrantReadWriteLock()
  private val readLock: ReentrantReadWriteLock.ReadLock = lock.readLock()
  private val writeLock: ReentrantReadWriteLock.WriteLock = lock.writeLock()
  private var memStatic: Long = 0

  def syncPut(key: Long, value: LongOpenHashSet): this.type = {
    writeLock.lock()
    try {
      memStatic += value.size + 1
      cache.put(key, value)
    } finally {
      writeLock.unlock()
    }

    this
  }

  def syncGet(key: Long): LongOpenHashSet = {
    readLock.lock()
    try {
      cache.get(key)
    } finally {
      readLock.unlock()
    }
  }

  def syncPut(batch: Long2ObjectOpenHashMap[LongOpenHashSet]): this.type = {
    writeLock.lock()
    try {
      memStatic += batch.size()
      val iter = batch.values().iterator()
      while (iter.hasNext) {
        memStatic += iter.next().size
      }
      cache.putAll(batch)
    } finally {
      writeLock.unlock()
    }

    this
  }

  def syncGet(keys: Array[Long]): Long2ObjectOpenHashMap[LongOpenHashSet] = {
    readLock.lock()
    val result = new Long2ObjectOpenHashMap[LongOpenHashSet]()
    try {
      keys.foreach { key =>
        if (cache.containsKey(key)) {
          result.put(key, cache.get(key))
        }
      }
    } finally {
      readLock.unlock()
    }

    result
  }

  def isCached(key: Long): Boolean = {
    readLock.lock()
    try {
      cache.containsKey(key)
    } finally {
      readLock.unlock()
    }
  }

  def isCached(keys: Array[Long]): Array[Boolean] = {
    readLock.lock()
    try {
      keys.map(cache.containsKey)
    } finally {
      readLock.unlock()
    }
  }

  def getUnCached(keys: Array[Long]): Array[Long] = {
    readLock.lock()
    try {
      keys.filterNot(key => cache.containsKey(key))
    } finally {
      readLock.unlock()
    }
  }

  def getUnCached(keys: LongOpenHashSet): Array[Long] = {
    readLock.lock()
    val buf = ListBuffer[Long]()
    try {
      val iter = keys.iterator()
      while (iter.hasNext) {
        val value = iter.nextLong()
        if (!cache.containsKey(value)) {
          buf.append(value)
        }
      }
      buf.toArray
    } finally {
      readLock.unlock()
    }
  }

  def getOrDefault(keys: Array[Long], default: Long2ObjectOpenHashMap[Array[Long]]):
  Long2ObjectOpenHashMap[LongOpenHashSet] = {
    readLock.lock()
    try {
      val result = new Long2ObjectOpenHashMap[LongOpenHashSet]
      keys.foreach { key =>
        if (cache.containsKey(key)) {
          result.put(key, cache.get(key))
        } else if (default.containsKey(key)) {
          result.put(key, CachedNeigh.arr2set(default.get(key)))
        } else {
          throw new Exception(s"Cannot find a value for key: $key")
        }
      }

      result
    } finally {
      readLock.unlock()
    }
  }

  def getOrDefault(keys: LongOpenHashSet, default: Long2ObjectOpenHashMap[Array[Long]]):
  Long2ObjectOpenHashMap[LongOpenHashSet] = {
    readLock.lock()
    try {
      val result = new Long2ObjectOpenHashMap[LongOpenHashSet]
      val iter = keys.iterator()
      while (iter.hasNext) {
        val key = iter.nextLong()
        if (cache.containsKey(key)) {
          result.put(key, cache.get(key))
        } else if (default.containsKey(key)) {
          result.put(key, CachedNeigh.arr2set(default.get(key)))
        } else {
          throw new Exception(s"Cannot find a value for key: $key")
        }
      }

      result
    } finally {
      readLock.unlock()
    }
  }

  def getMemStatic: Long = {
    readLock.lock()
    try {
      memStatic
    } finally {
      readLock.unlock()
    }

  }

  def clear(): Unit = {
    writeLock.lock()
    try {
      memStatic = 0
      cache = new Long2ObjectOpenHashMap[LongOpenHashSet]()
    } finally {
      writeLock.unlock()
    }
  }
}


object CachedNeigh {
  private var cache: CachedNeigh = _

  def arr2set(arr: Array[Long]): LongOpenHashSet = {
    val set = new LongOpenHashSet(arr.length)
    arr.foreach(x => set.add(x))

    set
  }

  def set2arr(set: LongOpenHashSet): Array[Long] = {
    val arr = new Array[Long](set.size())
    val iter = set.iterator()
    var idx = 0
    while (iter.hasNext) {
      arr(idx) = iter.nextLong()
      idx += 1
    }

    arr
  }

  def get(): CachedNeigh = classOf[CachedNeigh].synchronized {
    if (cache == null) {
      cache = new CachedNeigh(1024)
    }

    cache
  }

  def init(capacity: Int): CachedNeigh = classOf[CachedNeigh].synchronized {
    if (cache == null) {
      cache = new CachedNeigh(capacity)
    }

    cache
  }
}
