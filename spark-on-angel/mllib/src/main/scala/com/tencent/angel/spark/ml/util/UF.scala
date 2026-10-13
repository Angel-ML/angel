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

package com.tencent.angel.spark.ml.util

/**
 * Weighted quick-union by rank with path halving,
 */
final class UF(n: Int) {

  require(n >= 0, s"n($n) must be non-negative")

  private val parent: Array[Int] = Array.tabulate(n)(identity)
  private val rank: Array[Byte] = new Array[Byte](n)
  private var _count: Int = n

  def find(p: Int): Int = {
    validate(p)
    var root = p
    while (root != parent(root)) {
      parent(root) = parent(parent(root))
      root = parent(root)
    }
    root
  }

  def count: Int = _count

  def connected(p: Int, q: Int): Boolean = find(p) == find(q)

  def union(p: Int, q: Int): Unit = {
    val rootP = find(p)
    val rootQ = find(q)
    if (rootP != rootQ) {
      if (rank(rootP) < rank(rootQ)) {
        parent(rootP) = rootQ
      } else if (rank(rootP) > rank(rootQ)) {
        parent(rootQ) = rootP
      } else {
        parent(rootQ) = rootP
        rank(rootP) = (rank(rootP) + 1).toByte
      }
      _count -= 1
    }
  }

  private def validate(p: Int): Unit = {
    if (p < 0 || p >= parent.length) {
      throw new IllegalArgumentException(s"index $p is not between 0 and ${parent.length - 1}")
    }
  }
}
