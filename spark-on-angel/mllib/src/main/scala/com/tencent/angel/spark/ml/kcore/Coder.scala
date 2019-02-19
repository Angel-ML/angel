package com.tencent.angel.spark.ml.kcore

/**
 * code version num and core number by a integer
 */
private[kcore] object Coder {
  private val NUM_VERSION_BIT: Int = 6
  private val MAX_VERSION: Int = (1 << NUM_VERSION_BIT) - 1
  private val NUM_CORE_BIT: Int = 24
  private val MAX_CORE: Int = (1 << NUM_CORE_BIT) - 1

  def decodeCoreNumber(coreWithVersion: Int): Int = {
    coreWithVersion & MAX_CORE
  }

  def decodeVersion(coreWithVersion: Int): Int = {
    coreWithVersion >> NUM_CORE_BIT
  }

  def encodeVersion(version: Int): Int = {
    version << NUM_CORE_BIT
  }

  def isMaxVersion(version: Int): Boolean = {
    version == MAX_VERSION
  }

  def withVersion(version: Int): Int => Int = {
    val versionBit = encodeVersion(version)
    x => x | versionBit
  }

  def withNewVersion(version: Int): Int => Int = {
    val versionBit = encodeVersion(version)
    x => decodeCoreNumber(x) | versionBit
  }

}
