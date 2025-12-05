package com.tencent.angel.graph.embedding.struc2vec.test

import com.tencent.angel.graph.embedding.struc2vec.DTW

object TestDTW {
  def main(args: Array[String]): Unit = {
    //    val a = Array(1, 1, 1, 1, 2, 2, 3, 3, 4, 3, 2, 2, 1, 1, 1, 1)
    //    val b = Array(1, 1, 2, 2, 3, 3, 4, 4, 4, 4, 3, 3, 2, 2, 1, 1)
    //    println(DTW.compute(a, b))
    println(DTW.compute(Array(1, 1), Array(2)))
  }

}
