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

package com.tencent.angel.spark.ml.graph.kcore

/**
  * code version num and core number by a integer
  */
private[kcore] object Coder {
  private val NUM_VERSION_BIT: Int = 6
  private val MAX_VERSION: Int = (1 << NUM_VERSION_BIT) - 1
  private val NUM_CORE_BIT: Int = 24
  private val MAX_CORE: Int = (1 << NUM_CORE_BIT) - 1

  def decodeVersion(coreWithVersion: Int): Int = {
    coreWithVersion >> NUM_CORE_BIT
  }

  def isMaxVersion(version: Int): Boolean = {
    version == MAX_VERSION
  }

  def withVersion(version: Int): Int => Int = {
    val versionBit = encodeVersion(version)
    x => x | versionBit
  }

  def encodeVersion(version: Int): Int = {
    version << NUM_CORE_BIT
  }

  def withNewVersion(version: Int): Int => Int = {
    val versionBit = encodeVersion(version)
    x => decodeCoreNumber(x) | versionBit
  }

  def decodeCoreNumber(coreWithVersion: Int): Int = {
    coreWithVersion & MAX_CORE
  }

}
