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


package com.tencent.angel.spark.ml.psf.embedding


import java.util.Random

import org.apache.commons.logging.LogFactory

import com.tencent.angel.spark.ml.psf.embedding.NENegativeSample.{getSample, nextPos}


object NENegativeSample {
  private val LOG = LogFactory.getLog(classOf[NENegativeSample])
  private var samples: Array[Int] = _
  private var version = -1

  def init(maxValue: Int, seed: Int, version: Int): Unit = {
    if (version > this.version)
      this.synchronized {
        if (version > this.version) {
          val tableCapacity = Math.min(Math.max(maxValue / 10, 1000000), maxValue)
          LOG.info("initial sample table firstly, table capacity: " + tableCapacity)
          val rand = new Random(seed)
          samples = Array.fill(tableCapacity)(rand.nextInt(maxValue))
          this.version = version
        }
      }
  }

  def getSample(pos: Int): Int = samples(pos)

  def nextPos(pos: Int): Int = (pos + 1) % samples.length
}

class NENegativeSample(val maxValue: Int, val seed: Int) {
  private var position = Math.abs(seed) % NENegativeSample.samples.length

  def next(target: Int): Int = {
    var sample = getSample(position)
    position = nextPos(position)
    while (sample == target) {
      sample = getSample(position)
      position = nextPos(position)
    }
    sample
  }
}