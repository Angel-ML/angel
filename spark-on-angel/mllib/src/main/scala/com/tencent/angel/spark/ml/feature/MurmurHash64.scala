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
package com.tencent.angel.spark.ml.feature

object MurmurHash64 {

  def stringHash(str: String): Long = {
    val data = str.getBytes
    val length = data.length
    val seed = 0xe17a1465
    val m = 0xc6a4a7935bd1e995L
    val r = 47
    var h = (seed & 0xffffffffL) ^ (length * m)
    val length8 = length / 8
    for (i <- 0 until length8) {
      val i8 = i * 8
      var k = (data(i8 + 0) & 0xff).toLong + ((data(i8 + 1) & 0xff).toLong << 8) + ((data(i8 + 2) & 0xff).toLong << 16) + ((data(i8 + 3) & 0xff).toLong << 24) + ((data(i8 + 4) & 0xff).toLong << 32) + ((data(i8 + 5) & 0xff).toLong << 40) + ((data(i8 + 6) & 0xff).toLong << 48) + ((data(i8 + 7) & 0xff).toLong << 56)
      k *= m
      k ^= k >>> r
      k *= m
      h ^= k
      h *= m
    }
    if (length % 8 >= 7)
      h ^= (data((length & ~7) + 6) & 0xff).toLong << 48
    if (length % 8 >= 6)
      h ^= (data((length & ~7) + 5) & 0xff).toLong << 40
    if (length % 8 >= 5)
      h ^= (data((length & ~7) + 4) & 0xff).toLong << 32
    if (length % 8 >= 4)
      h ^= (data((length & ~7) + 3) & 0xff).toLong << 24
    if (length % 8 >= 3)
      h ^= (data((length & ~7) + 2) & 0xff).toLong << 16
    if (length % 8 >= 2)
      h ^= (data((length & ~7) + 1) & 0xff).toLong << 8
    if (length % 8 >= 1) {
      h ^= (data(length & ~7) & 0xff).toLong
      h *= m
    }
    h ^= h >>> r
    h *= m
    h ^= h >>> r
    h
  }
}
