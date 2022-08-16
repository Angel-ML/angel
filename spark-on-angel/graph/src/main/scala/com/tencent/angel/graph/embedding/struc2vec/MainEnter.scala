package com.tencent.angel.graph.embedding.struc2vec

import com.tencent.angel.graph.embedding.struc2vec.test.TestStructSimi.INF

object MainEnter {

  // Test Structure similarity
  // By computing the structure similarity of Barbell graph b(1,1)
  // Input graph adjacency matrix = [[0,1,INF],[1,0,1],[INF,1,0]]

  def main(args:Array[String]): Unit ={
        val adjMatrix = Array(Array(0, 1, INF), Array(1, 0, 1), Array(INF, 1, 0))
        val structureSimilarity = new StructureSimilarity(adjMatrix)
        structureSimilarity.compute()
        val result: Array[Array[Array[Double]]] = structureSimilarity.structSimi
        println("Layer1: ")
        for (i <- 0 to result.length - 1; j <- 0 to result(0).length - 1) {
          print((i,j) + " = " + result(0)(i)(j)+" ")
        }
        println()
        println("Layer2: ")
        for (i <- 0 to result.length - 1; j <- 0 to result(1).length - 1) {
          print( (i, j) + " = " + result(1)(i)(j) + " ")
        }
        println()
        println("Layer3: ")
        for (i <- 0 to result.length - 1; j <- 0 to result(2).length - 1) {
          print((i, j) + " = " + result(2)(i)(j) + " ")
        }
      }




}
