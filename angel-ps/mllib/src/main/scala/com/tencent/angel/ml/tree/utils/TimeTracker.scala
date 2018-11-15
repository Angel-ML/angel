package com.tencent.angel.ml.tree.utils

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
  * Time tracker implementation which holds labeled timers.
  */
class TimeTracker extends Serializable {

  private val starts: MutableHashMap[String, Long] = new MutableHashMap[String, Long]()

  private val totals: MutableHashMap[String, Long] = new MutableHashMap[String, Long]()

  /**
    * Starts a new timer, or re-starts a stopped timer.
    */
  def start(timerLabel: String): Unit = {
    val currentTime = System.nanoTime()
    if (starts.contains(timerLabel)) {
      throw new RuntimeException(s"TimeTracker.start(timerLabel) called again on" +
        s" timerLabel = $timerLabel before that timer was stopped.")
    }
    starts(timerLabel) = currentTime
  }

  /**
    * Stops a timer and returns the elapsed time in seconds.
    */
  def stop(timerLabel: String): Double = {
    val currentTime = System.nanoTime()
    if (!starts.contains(timerLabel)) {
      throw new RuntimeException(s"TimeTracker.stop(timerLabel) called on" +
        s" timerLabel = $timerLabel, but that timer was not started.")
    }
    val elapsed = currentTime - starts(timerLabel)
    starts.remove(timerLabel)
    if (totals.contains(timerLabel)) {
      totals(timerLabel) += elapsed
    } else {
      totals(timerLabel) = elapsed
    }
    elapsed / 1e9
  }

  /**
    * Print all timing results in seconds.
    */
  override def toString: String = {
    totals.map { case (label, elapsed) =>
      s"  $label: ${elapsed / 1e9}"
    }.mkString("\n")
  }
}
