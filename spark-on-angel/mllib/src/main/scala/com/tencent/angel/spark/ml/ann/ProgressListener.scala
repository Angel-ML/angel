package com.tencent.angel.spark.ml.ann

class ProgressListener(val func: (Int, Int) => Unit) {
  
  def updateProgress(workDone: Int, max: Int): Unit = {
    this.func.apply(workDone, max)
  }
  
}
