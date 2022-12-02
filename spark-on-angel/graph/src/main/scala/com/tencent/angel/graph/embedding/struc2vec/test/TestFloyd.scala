package com.tencent.angel.graph.embedding.struc2vec.test
import com.tencent.angel.graph.embedding.struc2vec.Floyd


object TestFloyd {
  val INF = Floyd.INF
  def main(args: Array[String]): Unit = {
    val len: Int = 3
    // initialize adjacency matrix
    val adjMatrix = Array.ofDim[Int](len, len)
    adjMatrix(0)(0) = 0
    adjMatrix(0)(1) = 1
    adjMatrix(0)(2) = INF
    adjMatrix(1)(0) = 1
    adjMatrix(1)(1) = 0
    adjMatrix(1)(2) = 1
    adjMatrix(2)(0) = INF
    adjMatrix(2)(1) = 1
    adjMatrix(2)(2) = 0

    val res = Floyd.hopCount(adjMatrix)
    for (i <- 0 to res.length - 1; j <- 0 to res(0).length - 1) {
      println("Element " + i + j + " = " + res(i)(j))
    }




  }
}
