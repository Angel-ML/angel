package com.tencent.angel.spark.ml.graph.data

import com.tencent.angel.utils.ArrayUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SwingCompute {

  def Swing(item_i:Long,item2items: Array[(Long,Array[Long])],alpha:Float = 0f,
            beta: Int = 0, gamma: Float = 0f)={
    val itemSet = new mutable.HashSet[Long]()
    val itemFreq = new mutable.HashMap[Long, Int]()
    val sumScore_ij = new mutable.HashMap[Long, Float]()
    val userIndex = new mutable.HashMap[Long, Int]()
    val itemJ2Users = new mutable.HashMap[Long, mutable.HashSet[Long]]()
    var j = 0
    item2items.map { case (user, userItems) =>
      userItems.map { f =>
        itemSet.add(f)
        itemFreq.put(f, itemFreq.getOrElse(f, 0) + 1)
      }
      userIndex.put(user, j)
      j += 1
    }

    val candidate = itemFreq.filter(_._2 > 1)
    item2items.map { case (user, userItems) =>
      userItems.map { f =>
        if (candidate.contains(f)) {
          if (itemJ2Users.contains(f)) {
            itemJ2Users(f).add(user)
          } else {
            val userSet = new mutable.HashSet[Long]()
            userSet.add(user)
            itemJ2Users.put(f, userSet)
          }
        }
      }
    }
    val temp = new mutable.HashMap[(Long, Long), Float]()
    for ((item, userSet) <- itemJ2Users) {
      if (item != item_i) {
        var i = 0
        val users = userSet.toArray
        if (users.length > 1000) {
          println(s"processing item pair ($item_i, $item) with ${users.length} user arrays.")
        }
        while (i < users.length - 1) {
          var j = i + 1
          val uItems = item2items(userIndex(users(i)))._2
          val wi = math.pow(uItems.length + beta, gamma)
          while (j < users.length) {
            val score = if (temp.contains((users(i), users(j)))) {
              temp((users(i), users(j)))
            } else {
              val vItems = item2items(userIndex(users(j)))._2
              val wj = wi * math.pow(vItems.length + beta, gamma)
              val score_ = Score(uItems, vItems, alpha, wj.toFloat)
              temp.update((users(i), users(j)), score_)
              score_
            }
            sumScore_ij.put(item, sumScore_ij.getOrElse(item, 0f) + score)
            j += 1
          }
          i += 1
        }
      }
    }
    (item_i, sumScore_ij.toArray.sortBy(-_._2))
  }
  
  def WeightedSwing(item_i:Long, item2items: Array[((Long, Float),Array[(Long, Float)])], alpha:Float = 0f,
                                  beta: Int = 0, gamma: Float = 0f)={
    val itemSet = new mutable.HashSet[Long]()
    val itemFreq = new mutable.HashMap[Long, Int]()
    val sumScore_ij = new mutable.HashMap[Long, Float]()
    val userIndex = new mutable.HashMap[Long, Int]()
    val itemJ2Users = new mutable.HashMap[Long, mutable.HashSet[Long]]()
    var j = 0
    
    item2items.foreach { case (user, userItems) =>
      userItems.map { f =>
        itemSet.add(f._1)
        itemFreq.put(f._1, itemFreq.getOrElse(f._1, 0) + 1)
      }
      userIndex.put(user._1, j)
      j += 1
    }
    
    val candidate = itemFreq.filter(_._2 > 1)
    item2items.map { case (user, userItems) =>
      userItems.map { f =>
        if (candidate.contains(f._1)) {
          if (itemJ2Users.contains(f._1)) {
            itemJ2Users(f._1).add(user._1)
          } else {
            val userSet = new mutable.HashSet[Long]()
            userSet.add(user._1)
            itemJ2Users.put(f._1, userSet)
          }
        }
      }
    }
    val temp = new mutable.HashMap[(Long, Long), Float]()
    for ((item, userSet) <- itemJ2Users) {
      if (item != item_i) {
        var i = 0
        val users = userSet.toArray
        if (users.length > 1000) {
          println(s"processing item pair ($item_i, $item) with ${users.length} user arrays.")
        }
        while (i < users.length - 1) {
          var j = i + 1
          val uItems = item2items(userIndex(users(i)))._2
          val wi = math.pow(uItems.length + beta, gamma)
          while (j < users.length) {
            val score = if (temp.contains((users(i), users(j)))) {
              temp((users(i), users(j)))
            } else {
              val vItems = item2items(userIndex(users(j)))._2
              val wj = wi * math.pow(vItems.length + beta, gamma)
              val score_ = WeightedScore(uItems, vItems, alpha, wj.toFloat)
              temp.update((users(i), users(j)), score_)
              score_
            }
            sumScore_ij.put(item, sumScore_ij.getOrElse(item, 0f) + score)
            j += 1
          }
          i += 1
        }
      }
    }
    (item_i, sumScore_ij.toArray.sortBy(-_._2))
  }

  def Score(Iu:Array[Long],Iv:Array[Long],alpha:Float, weight: Float = 1.0f): Float = {
    //    1.0f / (alpha+Iu.toSet.intersect(Iv.toSet).size)
    weight / (alpha + ArrayUtils.intersectCount(Iu, Iv))
  }
  
  // sort Iu Iv can make a nlogn algorithm
  def WeightedScore(Iu: Array[(Long, Float)], Iv:Array[(Long, Float)],
                                  alpha:Float, weight: Float = 1.0f): Float = {
    val IuSorted = Iu.sorted
    val IvSorted = Iv.sorted
    
    var i = 0
    var j = 0
    
    var sumW = 0.0f
    while (i < IuSorted.length && j < IvSorted.length) {
      if (IuSorted(i)._1 == IvSorted(j)._1) {
        sumW += Math.sqrt(IuSorted(i)._2 * IvSorted(j)._2).toFloat
        i += 1
        j += 1
      }
      else if (IuSorted(i)._1 < IvSorted(j)._1) {
        i += 1
      }
      else { // IuSorted(i)._1 > IvSorted(j)._1
        j += 1
      }
    }
    weight / (alpha + sumW)
  }

  def SwingV2(item_i:Long,item2items: Array[(Long,Array[Long])],
              alpha:Float = 0f, beta: Int = 5, gamma: Float = -0.3f): Array[(Long, Long, Float)] ={
    val itemSet = new mutable.HashSet[Long]()
    val itemFreq = new mutable.HashMap[Long,Int]()
    val userIndex = new mutable.HashMap[Long,Int]()
    val itemJ2Users = new mutable.HashMap[Long,mutable.HashSet[Long]]()
    var j = 0
    item2items.map { case (user, userItems) =>
      userItems.map { f =>
        if (f != item_i) {
          itemSet.add(f)
          itemFreq.put(f, itemFreq.getOrElse(f, 0) + 1)
        }
      }
      userIndex.put(user,j)
      j  += 1
    }

    val candidate  = itemFreq.filter(_._2 > 1)
    item2items.map{case (user,userItems) =>
      userItems.map{f=>
        if (candidate.contains(f)){
          if (itemJ2Users.contains(f)){
            itemJ2Users(f).add(user)
          }else {
            val userSet = new mutable.HashSet[Long]()
            userSet.add(user)
            itemJ2Users.put(f,userSet)
          }
        }
      }
    }
    val result = new ArrayBuffer[(Long, Long, Float)]()
    itemJ2Users.foreach { case (item, userSet) =>
      var i = 0
      val users = userSet.toArray
      if (users.length > 10000) {
        println(s"processing item pair ($item_i, $item) with ${users.length} user arrays.")
      }
      var simScore = 0f
      while (i < users.length - 1) {
        var j = i + 1
        val uItems = item2items(userIndex(users(i)))._2
        val wi = math.pow(uItems.length + beta, gamma)
        while (j < users.length) {
          val vItems = item2items(userIndex(users(j)))._2
          val wj = wi * math.pow(vItems.length + beta, gamma)
          val score = Score(uItems, vItems, alpha, wj.toFloat)
          simScore += score
          j += 1
        }
        i += 1
      }
      result += ((item_i, item, simScore))
//      result += ((item, item_i, simScore))
    }
    result.toArray
  }

  // filter out super item pairs by tagging the score as -1f
  def SwingV4(item_i:Long,item2items: Array[(Long,Array[Long])],
              alpha:Float = 0f, beta: Int = 5, gamma: Float = -0.3f,
              superUserSetThreshold: Int): Array[(Long, Long, Float)] ={
    val itemSet = new mutable.HashSet[Long]()
    val itemFreq = new mutable.HashMap[Long,Int]()
    val userIndex = new mutable.HashMap[Long,Int]()
    val itemJ2Users = new mutable.HashMap[Long,mutable.HashSet[Long]]()
    var j = 0
    item2items.map { case (user, userItems) =>
      userItems.map { f =>
        if (f != item_i) {
          itemSet.add(f)
          itemFreq.put(f, itemFreq.getOrElse(f, 0) + 1)
        }
      }
      userIndex.put(user,j)
      j  += 1
    }

    val candidate  = itemFreq.filter(_._2 > 1)
    item2items.map{case (user,userItems) =>
      userItems.map{f=>
        if (candidate.contains(f)){
          if (itemJ2Users.contains(f)){
            itemJ2Users(f).add(user)
          }else {
            val userSet = new mutable.HashSet[Long]()
            userSet.add(user)
            itemJ2Users.put(f,userSet)
          }
        }
      }
    }
    val result = new ArrayBuffer[(Long, Long, Float)]()
    itemJ2Users.foreach { case (item, userSet) =>
      if (userSet.size > superUserSetThreshold){
        println(s"item pair ($item_i, $item) has ${userSet.size} common users.")
        result += ((item_i, item, -1f))
      } else {
        var i = 0
        val users = userSet.toArray
        if (users.length > 10000) {
          println(s"processing item pair ($item_i, $item) with ${users.length} user arrays.")
        }
        var simScore = 0f
        while (i < users.length - 1) {
          var j = i + 1
          val uItems = item2items(userIndex(users(i)))._2
          val wi = math.pow(uItems.length + beta, gamma)
          while (j < users.length) {
            val vItems = item2items(userIndex(users(j)))._2
            val wj = wi * math.pow(vItems.length + beta, gamma)
            val score = Score(uItems, vItems, alpha, wj.toFloat)
            simScore += score
            j += 1
          }
          i += 1
        }
        result += ((item_i, item, simScore))
//        result += ((item, item_i, simScore))
      }
    }
    result.toArray
  }

  def main(args: Array[String]): Unit = {

    val s = "11001\t21001\n11001\t21002\n11001\t21003\n11001\t21004\n11001\t21005\n11002\t21001\n11002\t21002\n11002\t21003\n11002\t21004\n11002\t21005\n11003\t21001\n11003\t21002\n11003\t21003\n11004\t21001\n11004\t21002\n11004\t21003\n11005\t21001\n11005\t21002\n11005\t21003\n11005\t21004\n11005\t21005\n11006\t21003\n11006\t21004\n11006\t21005"
    val itemUser = s.trim.split("\n").map { case userItem =>
      val t = userItem.trim.split("\t")
      (t(1).toLong, t(0).toLong)
    }
    val itemUsers = itemUser.groupBy(_._1).map(x => (x._1, x._2.map(_._2))).toArray
    val userItems = itemUser.map(x => (x._2, x._1)).groupBy(_._1).map(x => (x._1, x._2.map(_._2)))

    val item2item = itemUsers.map { case (item, users) =>
      (item, users.map(t => (t, userItems.getOrElse(t, Array[Long]()))))
    }.sortBy(_._1)

    val a = item2item(0)
    val b = item2item(1)
    println(a._1, b._1)

    val res = Swing(a._1, a._2)
    println(res._1, res._2.map(x => x._1 + "," + x._2).mkString("\t"))
  }
}
