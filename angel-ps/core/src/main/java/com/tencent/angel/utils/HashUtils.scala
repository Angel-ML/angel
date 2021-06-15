package com.tencent.angel.utils

import java.nio.{ByteBuffer, ByteOrder}
import java.security.MessageDigest

object HashUtils {

  private val md5Digest: MessageDigest = MessageDigest.getInstance("MD5")

  /**
    * Get the FNV1_32_HASH code of the given key.
    */
  def getFNV1_32_HashCode(str: String): Int = {
    val p = 16777619
    var hash = 2166136261L.toInt
    var i = 0
    while (i < str.length) {
      hash = (hash ^ str.charAt(i)) * p
      i += 1
    }
    hash += hash << 13
    hash ^= hash >> 7
    hash += hash << 3
    hash ^= hash >> 17
    hash += hash << 5
    // If the calculated value is negative, take its absolute value
    if (hash < 0) hash = Math.abs(hash)
    hash
  }

  def getFNV1_32_HashCode(intKey: Int): Int = {
    getFNV1_32_HashCode(intKey.toString)
  }

  def getFNV1_32_HashCode(longKey: Long): Int = {
    getFNV1_32_HashCode(longKey.toString)
  }

  /**
    * Get the MurmurHash code of the given key.
    */
  def getMurmurHashCode(str: String):Int = {
    val buf: ByteBuffer = ByteBuffer.wrap(str.getBytes())
    val seed: Int = 0x1234ABCD

    val byteOrder: ByteOrder = buf.order()
    buf.order(ByteOrder.LITTLE_ENDIAN)

    val m: Long = 0xc6a4a7935bd1e995L
    val r: Int = 47

    var h: Long = seed ^ (buf.remaining() * m)

    var k: Long = 0
    while (buf.remaining() >= 8) {
      k = buf.getLong()

      k *= m
      k ^= k >>> r
      k *= m

      h ^= k
      h *= m
    }

    if (buf.remaining() > 0) {
      val finish: ByteBuffer = ByteBuffer.allocate(8).order(
        ByteOrder.LITTLE_ENDIAN)
      // for big-endian version, do this first:
      // finish.position(8-buf.remaining());
      finish.put(buf).rewind()
      h ^= finish.getLong()
      h *= m
    }
    h ^= h >>> r
    h *= m
    h ^= h >>> r

    buf.order(byteOrder)
    var hash = (h & 0xffffffffL).toInt
    // If the calculated value is negative, take its absolute value
    if (hash < 0) hash = Math.abs(hash)
    hash
  }

  def getMurmurHashCode(intKey: Int):Int = {
    getKetamaHashCode(intKey.toString)
  }

  def getMurmurHashCode(longKey: Long):Int = {
    getKetamaHashCode(longKey.toString)
  }

  /**
    * Get the KetamaHash code of the given key.
    */
  def getKetamaHashCode(key: String): Int = {
    val bKey = computeMd5(key)
    val rv: Long = ((bKey(3) & 0xFF).toLong << 24)|
      ((bKey(2) & 0xFF).toLong << 16) |
      ((bKey(1) & 0xFF).toLong << 8) |
      (bKey(0) & 0xFF)
    var hash = (rv & 0xffffffffL).toInt
    // If the calculated value is negative, take its absolute value
    if (hash < 0) hash = Math.abs(hash)
    hash
  }

  /**
    * Get the md5 of the given key.
    */
  private def computeMd5(k: String): Array[Byte] = {
    var md5: MessageDigest = md5Digest
    try {
      md5 = md5Digest.clone().asInstanceOf[MessageDigest]
    } catch {
      case e: CloneNotSupportedException =>
        throw new RuntimeException("clone of MD5 not supported", e);
    }
    md5.update(k.getBytes())
    md5.digest()
  }

  def getKetamaHashCode(intKey: Int): Int = {
    getKetamaHashCode(intKey.toString)
  }

  def getKetamaHashCode(longKey: Long): Int = {
    getKetamaHashCode(longKey.toString)
  }

}
