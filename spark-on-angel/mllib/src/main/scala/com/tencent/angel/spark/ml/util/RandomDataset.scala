package com.tencent.angel.spark.ml.util

import java.io.{File, PrintWriter}

import scala.util.Random

object RandomDataset {
  
  def genEmbedding(path: String, dim:Int = 64, num: Int = 1000000): Unit = {
    val writer = new PrintWriter(new File(path))
    for (i <- 0 until num) {
      val vec = new Array[Float](dim)
      val vec2 = vec.map(_ => Random.nextFloat())
      writer.println(s"$i:${vec2.mkString(" ")}")
    }
    
    writer.close()
  }
  
  def genIdQueries(path: String, num: Int = 200): Unit = {
    val writer = new PrintWriter(new File(path))
    writer.println((0 until num).toArray.mkString(" "))
    writer.close()
  }
  
  def genJaccardData(path: String, num: Int = 200): Unit = {
    val writer = new PrintWriter(new File(path))
    for (i <- 0 until num) {
      val vec = new Array[Int](Math.max(5, Random.nextInt(20)))
      val vec2 = vec.map(_ => Random.nextInt(1000))
      writer.println(s"${Random.nextString(5)}\t${vec2.mkString(",")}")
    }
  
    writer.close()
  }
  
  def main(args: Array[String]): Unit = {
    val path = "data/embeddingData/"
    
    genEmbedding(path + "vectorQueries", 64, 200)
    
//    genIdQueries(path + "idQueries")
//
//    genEmbedding(path + "vectorQueries")
//    genJaccardData(path + "jaccardTest")
    
  }
}
