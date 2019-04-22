package com.tencent.angel.spark.ml.graph.utils

import java.util.Random

import org.apache.spark.SparkPrivateClassProxy
import org.apache.spark.util.random.RandomSampler

import scala.reflect.ClassTag

abstract class WeightedRandomSampler[T: ClassTag, U: ClassTag] extends RandomSampler[(T, Float), U] {

  protected var fraction = 0.0

  override def sample(items: Iterator[(T, Float)]): Iterator[U] = {
    items.filter(x => sample(x._2) > 0).asInstanceOf[Iterator[U]]
  }

  def sample(weight: Float): Int

  override def sample(): Int = ???

  def setFraction(fraction: Double): Unit = {
    require(
      fraction >= (0.0 - 1e-6)
        && fraction <= (1.0 + 1e-6),
      s"Sampling fraction ($fraction) must be on interval [0, 1]")
    this.fraction = fraction
  }

  override def clone: WeightedRandomSampler[T, U] = ???
}

class NaiveWeightedBernoulliSampler[T: ClassTag] extends WeightedRandomSampler[T, (T, Float)] {

  private val rng: Random = SparkPrivateClassProxy.getXORShiftRandom(System.nanoTime)

  override def setSeed(seed: Long): Unit = rng.setSeed(seed)

  def sample(weight: Float): Int = {
    if (fraction <= 0.0) {
      0
    } else if (fraction >= 1.0) {
      1
    } else {
      if (rng.nextDouble() <= fraction * weight) {
        1
      } else {
        0
      }
    }
  }

  override def clone: NaiveWeightedBernoulliSampler[T] = new NaiveWeightedBernoulliSampler[T]
}