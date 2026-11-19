package com.tencent.angel.spark.ml.embedding

import java.util.Random

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.ehcache.sizeof.SizeOf
import org.scalatest.funsuite.AnyFunSuite

class TempTest extends AnyFunSuite{
  test("size") {
    val map = new Long2ObjectOpenHashMap[Array[Long]]()

    val rand = new Random()
    val sizeOf = SizeOf.newInstance()
    val mapLen = rand.nextInt(1000)
    var count = 0 // mapLen
    (0 until mapLen).foreach{ key =>
      val length = rand.nextInt(1000)
      val value = new Array[Long](length)
      value.indices.foreach{ i =>
        value(i) = rand.nextLong()
      }
      count += length + 1
      map.put(key.toLong, value)
    }

    val realSize = sizeOf.deepSizeOf(map)
    val estSize = count * 8

    println(f"$estSize, $realSize, ${1.0*estSize/realSize}%.3f")

  }
}
