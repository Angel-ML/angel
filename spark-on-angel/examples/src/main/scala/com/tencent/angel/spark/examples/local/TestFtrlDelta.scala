package com.tencent.angel.spark.examples.local

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.OptFuncs
import com.tencent.angel.ml.math2.vector.IntFloatVector

object TestFtrlDelta {

  def main(args: Array[String]): Unit = {
    val v1 = VFactory.sparseFloatVector(10)
    v1.set(0, 0.1F)
    v1.set(1, 0.1F)

    val v2 = VFactory.sparseFloatVector(10)
    v2.set(1, 0.2F)
    v2.set(3, 0.2F)

    val v3 = OptFuncs.ftrldelta(v1, v2, 0.1)
    for (i <- 0 until 10) {
      if (v3.asInstanceOf[IntFloatVector].hasKey(i)) {
        println(s"$i ${v3.asInstanceOf[IntFloatVector].get(i)}")
      }
    }
  }
}
