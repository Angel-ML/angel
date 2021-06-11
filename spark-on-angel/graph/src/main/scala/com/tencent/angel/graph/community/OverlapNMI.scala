package com.tencent.angel.graph.community

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

/**
 * 标准化互信息NMI
 * 参考链接：http://www.yalewoo.com/2017/05/20/nmi_normalized_mutual_information_of_overlapping_community_detection/
 */
class OverlapNMI() extends Serializable {
  var nodesNum = 0.toLong
  private val random = new Random()
  private val SPLIT = '@'
  def h(p: Double) : Double = {
    if(p > 0)
      -1.0 * p * math.log(p) / math.log(2)
    else 0.0
  }

  // 信息熵：H(Xi)
  def H(community: Array[Long]) : Double = {
    val P1 = 1.0 * community.size / nodesNum
    val P0 = 1 - P1

    h(P1) + h(P0)
  }
  // 整体信息熵：H(X)
  def sumH(communities: RDD[Array[Long]]) : Double = {
    val res = communities.map(H)

    res.reduce(_+_)
  }

  // 联合熵：H(Xi, Yj)
  def H_Xi_join_Yj(community1: Array[Long], community2: Array[Long]) : Double = {
    val C1 = community1.toSet
    val C2 = community2.toSet

    val P11 = 1.0 * C1.&(C2).size / nodesNum
    val P10 = 1.0 * C1.&~(C2).size / nodesNum
    val P01 = 1.0 * C2.&~(C1).size / nodesNum
    val P00 = 1.0 - P11 - P10 - P01

    if (h(P11) + h(P00) >= h(P01) + h(P10))
      h(P11) + h(P10) + h(P01) + h(P00)
    else
      H(community1) + H(community2)
  }

  // 条件熵：H(Xi | Yj)
  def H_Xi_given_Yj(community1: Array[Long], community2: Array[Long]) : Double = {
    H_Xi_join_Yj(community1, community2) - H(community2)
  }

  // 条件熵：H(X | Y)
  def H_X_given_Y(detectCommunities: RDD[Array[Long]], realCommunities: RDD[Array[Long]]) : Double = {
    val allCommunities = detectCommunities.cartesian(realCommunities)
    val H_C_given_C = allCommunities.mapPartitions(iter => {
      for((community1, community2) <- iter) yield {
        val res = H_Xi_given_Yj(community1, community2)

        (community1.toString, res)
      }
    }).reduceByKey((h1, h2) => math.min(h1, h2))

    H_C_given_C.values.reduce(_+_)
  }

  // 条件熵：H(X | Y)_normal
  def H_X_given_Y_normal(detectCommunities: RDD[Array[Long]], realCommunities: RDD[Array[Long]]) : Double = {
    val allCommunities = detectCommunities.cartesian(realCommunities)
    val H_C_given_C_normal = allCommunities.mapPartitions(iter => {
      for((community1, community2) <- iter) yield {
        val res = H_Xi_given_Yj(community1, community2) / H(community1)

        (community1.toString, res)
      }
    }).reduceByKey((h1, h2) => math.min(h1, h2))

    H_C_given_C_normal.values.reduce(_+_)*1.0 / detectCommunities.count()
  }

  def NMI_LFK(detectCommunities: RDD[Array[Long]],
              realCommunities: RDD[Array[Long]],
              verticesNum: Long) : Double = {
    if (detectCommunities.count() == 0 || realCommunities.count() == 0)
      return 0.0

    nodesNum = verticesNum

    1.0 - 0.5 * (H_X_given_Y_normal(detectCommunities, realCommunities) + H_X_given_Y_normal(realCommunities, detectCommunities))
  }

  // 互信息：I(X : Y)
  def I(detectCommunities: RDD[Array[Long]], realCommunities: RDD[Array[Long]]) : Double = {
    0.5 * (sumH(detectCommunities) + sumH(realCommunities) - H_X_given_Y(detectCommunities, realCommunities) - H_X_given_Y(realCommunities, detectCommunities))
  }

  def NMI_max(detectCommunities: RDD[Array[Long]],
              realCommunities: RDD[Array[Long]],
              verticesNum: Long) : Double= {
    if (detectCommunities.count() == 0 || realCommunities.count() == 0)
      return 0.0

    nodesNum = verticesNum

    I(detectCommunities, realCommunities) / math.max(sumH(detectCommunities), sumH(realCommunities))
  }

  def pureRate(overlapCommunities: RDD[(Long,Array[Long])],
               realCommunities: RDD[(Long,Array[String])],
               communitySize: RDD[(Long,Int)]): (Double,RDD[(Long,Double)]) = {

    val id2com = overlapCommunities.flatMap(x=> {
      val (com, vids) = x
      var res = new mutable.ArrayBuffer[(Long,Long)]
      vids.foreach(vid => res += ((vid,com)))
      res
    })
    val com2label = id2com.join(realCommunities).mapPartitions(item => {
      for((vid,(com,labels)) <- item) yield {
        (com,labels)
      }
    }).reduceByKey(_++_)

    val com2label2count = com2label.mapPartitions(item => {
      for((com,labels) <- item) yield {
        val newMap = scala.collection.mutable.HashMap[String, Int]()
        labels.foreach(l => {
          if (newMap.contains(l)) newMap(l) = newMap(l) + 1
          else newMap(l) = 1
        })
        (com, newMap.maxBy(_._2)._2)
      }
    })

    val res = com2label2count.join(communitySize).mapPartitions(item => {
      for((com,(label_max,node_sum)) <- item) yield {
        (com,label_max*1.0/node_sum)
      }
    })
    (res.values.reduce(_+_)*1.0/res.count(), res)

  }

}

object OverlapNMI{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      "local",
      "LPA"
    )

    val detectCommunities = sc.parallelize(Array(Iterable(1.toLong,2.toLong,3.toLong,4.toLong),
      Iterable(3.toLong,4.toLong,5.toLong,6.toLong,7.toLong),
      Iterable(6.toLong,7.toLong,8.toLong,9.toLong)))
    val realCommunities = sc.parallelize(Array(Iterable(1.toLong,2.toLong,3.toLong,4.toLong),
      Iterable(5.toLong,6.toLong,7.toLong,8.toLong,9.toLong),
      Iterable(7.toLong,8.toLong,9.toLong)))

    val a = sc.parallelize(Array(Iterable(1.toLong,2.toLong,3.toLong,4.toLong),
      Iterable(3.toLong,4.toLong,5.toLong,6.toLong,7.toLong),
      Iterable(6.toLong,7.toLong,8.toLong,9.toLong)))
    val b = sc.parallelize(Array(Iterable(1.toLong,2.toLong,3.toLong,4.toLong),
      Iterable(5.toLong,6.toLong,7.toLong,8.toLong,9.toLong),
      Iterable(7.toLong,8.toLong,9.toLong)))

    val vidLabels = sc.parallelize(Array((1.toLong,Array("a")), (2.toLong,Array("a")), (3.toLong,Array("a")),
      (4.toLong,Array("a")), (5.toLong,Array("b")), (6.toLong,Array("b")),
      (7.toLong,Array("b","c")), (8.toLong,Array("b","c")), (9.toLong,Array("b","c"))))

    val console = new OverlapNMI()

  }


}