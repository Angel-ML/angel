package com.tencent.angel.graph.embedding.struc2vec.test

import com.tencent.angel.graph.embedding.struc2vec._

object TestStructSimi {
  val INF = Floyd.INF
  def main(args:Array[String]): Unit ={
    val adjMatrix = Array(Array(0,1,INF),Array(1,0,1),Array(INF,1,0))
    val struct_simi = new StructureSimilarity(adjMatrix)
    val diam = struct_simi.diam
    val degrees = struct_simi.degrees
    val hc = struct_simi.hopCountResult
    val ring = struct_simi.getHopRingK(1,1)
    println(diam)
    for (i <- 0 to hc.length - 1; j <- 0 to hc(0).length - 1) {
      println("Element " + i + j + " = " + hc(i)(j))
    }

   println(struct_simi.getHopRingK(0,0)(0))
   println(struct_simi.getHopRingK(1,0)(0))

//   println(DTW.compute(struct_simi.getHopRingK(0,0),struct_simi.getHopRingK(0,0)))

//    for (i <- 0 to degrees.length - 1)
//      println("Element " + i  + " = " + degrees(i))

//    for (i <- 0 to ring.length - 1)
//      println("ring Element " + i + " = " + ring(i))

//    println(ring.length)



  }

}
