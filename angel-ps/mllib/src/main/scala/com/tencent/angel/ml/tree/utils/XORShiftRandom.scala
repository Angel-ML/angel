package com.tencent.angel.ml.tree.utils

import java.nio.ByteBuffer
import java.util.{Random => JavaRandom}

import scala.util.hashing.MurmurHash3

/**
  * This class implements a XORShift random number generator algorithm
  * Source:
  * Marsaglia, G. (2003). Xorshift RNGs. Journal of Statistical Software, Vol. 8, Issue 14.
  * @see <a href="http://www.jstatsoft.org/v08/i14/paper">Paper</a>
  * This implementation is approximately 3.5 times faster than
  * {@link java.util.Random java.util.Random}, partly because of the algorithm, but also due
  * to renouncing thread safety. JDK's implementation uses an AtomicLong seed, this class
  * uses a regular Long. We can forgo thread safety since we use a new instance of the RNG
  * for each thread.
  */
class XORShiftRandom(init: Long) extends JavaRandom(init) {

  def this() = this(System.nanoTime)

  private var seed = XORShiftRandom.hashSeed(init)

  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
  override protected def next(bits: Int): Int = {
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    seed = nextSeed
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long) {
    seed = XORShiftRandom.hashSeed(s)
  }
}

/** Contains benchmark method and main method to run benchmark of the RNG */
object XORShiftRandom {

  /** Hash seeds to have 0/1 bits throughout. */
  private def hashSeed(seed: Long): Long = {
    val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
    val lowBits = MurmurHash3.bytesHash(bytes)
    val highBits = MurmurHash3.bytesHash(bytes, lowBits)
    (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
  }

}
