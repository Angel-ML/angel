package com.tencent.angel.ml.warplda

import java.util

/**
  * Sample Multinomial Distribution in O(1) with O(K) Build Time
  * Created by chris on 8/2/17.
  * @param  Array[count]
  */
class AliasTable(val count:Array[Int]) extends IntSampling{
  private final val wordTopicCount = count.zipWithIndex.filter(_._1 > 0)
  private final val length:Int = wordTopicCount.length
  private final val alias = Array.ofDim[Int](length)
  private final val sum:Float = wordTopicCount.map(_._1).sum.toFloat
  private final val probability:Array[Float] = wordTopicCount.map(f => f._1/sum*length)

  def build():Unit = {
    val small = new util.ArrayDeque[Int]()
    val large = new util.ArrayDeque[Int]()
    (0 until length) foreach {i=>
      if(probability(i) < 1.0) small.add(i) else large.add(i)
    }

    while(!small.isEmpty) {
      val less = small.pop()
      val more = large.pop()
      alias(less) = more
      probability(more) -= 1f - probability(less)
      if( probability(more) < 1f) small.add(more) else large.add(more)
    }
  }
  def apply():Int = {
    val column = r.nextInt(length)
    if(r.nextDouble() < probability(column)) wordTopicCount(column)._2 else wordTopicCount(alias(column))._2
  }
}

/*
object AliasTable {
  def main(args: Array[String]): Unit = {
    val wordTopicCount = Array((100,1),(200,1),(1000,3),(400,4))
    val aliasTable = new AliasTable(wordTopicCount)
    aliasTable.build()
    val z = ArrayBuffer[Int]()
    (0 until 1000000) foreach { i =>
      z.append(aliasTable.apply())
    }
    println(z.count(f=>f==1))
    println(z.count(f=>f==5))
    println(z.count(f=>f==3))
    println(z.count(f=>f==7))
  }
}
*/
