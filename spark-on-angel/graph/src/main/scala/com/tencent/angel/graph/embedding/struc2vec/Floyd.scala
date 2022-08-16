package com.tencent.angel.graph.embedding.struc2vec

object Floyd {
  val INF = Int.MaxValue
  def hopCount(adj:Array[Array[Int]]):Array[Array[Int]] = {
    // self implementation of hop-count demo based on Floyd algorithm
    // Assume that adj is a square matrix
    val len = adj.length
    val result: Array[Array[Int]] = Array.ofDim(len, len)
    adj.copyToArray(result)
    for(k<- 0 until len;i <- 0 until len; j<- 0 until len )
        if(!result(i)(k).equals(INF) && !result(k)(j).equals(INF))
             result(i)(j) = Math.min(result(i)(j),result(i)(k)+result(k)(j))
    result
  }
}
