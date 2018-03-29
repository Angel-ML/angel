/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.warplda

import java.util
import java.util.Random

/**
  * Sample Multinomial Distribution in O(1) with O(K) Build Time
  * Created by chris on 8/2/17.
  *
  * @param count
  */
class AliasTable(val count: Array[Int]) {
  private final val wordTopicCount = count.zipWithIndex.filter(_._1 > 0)
  private final val length: Int = wordTopicCount.length
  private final val alias = Array.ofDim[Int](length)
  private final val sum: Float = wordTopicCount.map(_._1).sum.toFloat
  private final val probability: Array[Float] = wordTopicCount.map(f => f._1 / sum * length)
  private final val r: Random = new Random(System.currentTimeMillis())

  def build(): Unit = {
    val small = new util.ArrayDeque[Int]()
    val large = new util.ArrayDeque[Int]()
    (0 until length) foreach { i =>
      if (probability(i) < 1.0) small.add(i) else large.add(i)
    }

    while (!small.isEmpty) {
      val less = small.pop()
      val more = large.pop()
      alias(less) = more
      probability(more) -= 1f - probability(less)
      if (probability(more) < 1f) small.add(more) else large.add(more)
    }
  }

  def apply(): Int = {
    val column = r.nextInt(length)
    if (r.nextDouble() < probability(column)) wordTopicCount(column)._2 else wordTopicCount(alias(column))._2
  }
}
