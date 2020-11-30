/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.graph.utils

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