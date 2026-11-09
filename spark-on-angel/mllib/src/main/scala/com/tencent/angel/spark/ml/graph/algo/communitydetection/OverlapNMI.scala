package com.tencent.angel.spark.ml.graph.algo.communitydetection

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

  //  // 条件熵：H(Xi | Y)
  //  def H_Xi_given_Y(community1: Array[Long], realCommunities: RDD[Array[Long]]) : Double = {
  //    val res = realCommunities.map(H_Xi_given_Yj(community1, _))
  //
  //    res.min()
  //  }
  //  // 标准条件熵：H(Xi | Y)_normal
  //  def H_Xi_given_Y_normal(community1: Array[Long], realCommunities: RDD[Array[Long]]) : Double = {
  //    H_Xi_given_Y(community1, realCommunities) / H(community1)
  //  }

  //  // 条件熵：H(X | Y)
  //  def H_X_given_Y(detectCommunities: RDD[Array[Long]], realCommunities: RDD[Array[Long]]) : Double = {
  //    val res = detectCommunities.map(H_Xi_given_Y(_, realCommunities))
  //
  //    res.sum()
  //  }
  //  // 标准条件熵：H(X | Y)_normal
  //  def H_X_given_Y_normal(detectCommunities: RDD[Array[Long]], realCommunities: RDD[Array[Long]]) : Double = {
  //    val res = detectCommunities.map(H_Xi_given_Y_normal(_, realCommunities))
  //
  //    res.sum() / detectCommunities.count()
  //  }

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

//  def run(detectCommunities: RDD[Iterable[Long]],
//          realCommunities: RDD[Iterable[Long]],
//          verticesNum: Long) = {
//    val nmi_LFK = NMI_LFK(detectCommunities, realCommunities, verticesNum)
//    val nmi_max = NMI_max(detectCommunities, realCommunities, verticesNum)
//
//    (nmi_LFK, nmi_max)
//  }

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

  // 模块度计算
  /*
  def modularity(LPAGraph: Graph[VertexData, Double],
                 totalEdgeWeight: Double, loopCount: Int) : Double = {
    val communitySigmaTot = twoStageReduce(LPAGraph.vertices.flatMap(vertex => {
      var result =  new mutable.ArrayBuffer[(Long, Double)]
      val nodeweight = vertex._2.nodeWeight
      vertex._2.estimateLabel.keys.foreach(label => {
        result += ((label,nodeweight))
      })
      result
    }))
    val internalWeight = twoStageReduce(LPAGraph.triplets.flatMap(et => {
      var result = new mutable.ArrayBuffer[(Long, Double)]
      val labels1 = et.srcAttr.estimateLabel.keys
      val labels2 = et.dstAttr.estimateLabel.keys
      labels1.foreach(label1 => {
        labels2.foreach(label2 => {
          if(label1 == label2)
            result += ((label1, et.attr*2))
          else
            result += ((label1, 0.0),(label2, 0.0))
        })
      })
      result
    }))
    val Q = internalWeight.join(communitySigmaTot).mapPartitions(iter => {
      for((label, (internal, sigmaTot)) <- iter) yield {
        val tmp = (sigmaTot/totalEdgeWeight) * (sigmaTot/totalEdgeWeight)
        val q = internal/totalEdgeWeight - tmp
        (random.nextInt(100),q)
      }
    })
      .reduceByKey(_+_).mapPartitions(vertexIter => for(r <- vertexIter) yield  r._2 ).reduce(_+_)

    Q
  }

  // 多阶段聚合，本质上是对key分桶操作
  def twoStageReduce(pairs: RDD[(Long, Double)]) : RDD[(Long, Double)] ={

    pairs.mapPartitions(Iter => {
      for((k,v) <- Iter) yield (k.toString + SPLIT + random.nextInt(100000), v)
    }).reduceByKey(_+_)
      .mapPartitions(Iter =>{for((k,v) <- Iter) yield (k.split(SPLIT)(0).toLong, v)
      }).reduceByKey(_+_)
  }

  // 按照比例选取有效标签
  def validLabelInfo(sc: SparkContext, graph: Graph[VertexData, (Double, Double, Double)],
                     loopCount: Int) : (RDD[(Long, Array[Long])], Graph[VertexData, Double]) = {
    val hiveCtx = SparkSession.builder().config(conf = sc.getConf).enableHiveSupport().getOrCreate()
    import hiveCtx.implicits._

    val estimateLabelLabels = graph.vertices.mapPartitions(iter => {
      for((vid, vData) <- iter) yield {
        (vid, vData.estimateLabel.keys.toArray)
      }
    })

    // 重叠社区信息
    val communities = estimateLabelLabels.flatMap(vertex => {
      val (vid, labels) = vertex
      var res =  new mutable.ArrayBuffer[(String, scala.collection.mutable.HashSet[Long])]
      labels.foreach(label => { res += ((label.toString + SPLIT + random.nextInt(10000), scala.collection.mutable.HashSet(vid)))})
      res
    }).reduceByKey(_++_).mapPartitions(Iter =>{for((k,v) <- Iter) yield (k.split(SPLIT)(0).toLong, v)})
      .reduceByKey(_++_).persist(StorageLevel.MEMORY_AND_DISK_SER).setName("communities")
    communities.foreach(_ => Unit)
    //    println("communities"+communities.count())
    val communityID = communities.mapPartitions(Iter =>{ for((k,v) <- Iter) yield (k,true)})
    val communityDual = communityID.cartesian(communityID).filter(x => x._1._1 > x._2._1)
      .mapPartitions(eIter =>{ for (((label1, flag1),(label2, flag2)) <- eIter) yield (label1,label2) })
    //    communities.mapPartitions(Iter =>{ for((k,v) <- Iter) yield k})
    //                        .toDF("accountid").createOrReplaceTempView("temptable2")
    //    val communityDual = hiveCtx.sql(s"select * from temptable2 t1 join temptable2 t2 on t1.accountid > t2.accountid").rdd
    //      .mapPartitions(eIter =>{ for (row <- eIter) yield (row.get(0).toString.toLong, row.get(1).toString.toLong) })
    communityDual.foreach(_ => Unit)
    //    println("communityDual"+communityDual.count())
    val tmpComs = communityDual.join(communities).mapPartitions(iter => for((label1, (label2, vids)) <- iter) yield (label2, (label1, vids)))
    //    tmpComs.foreach(_ => Unit)

    val duplicateComs  = tmpComs.join(communities).mapPartitions(iter => {
      for( (label2, ((label1, idlist1), idlist2)) <- iter) yield {
        var res = false
        var label = label1
        val len1 = idlist1.size
        val len2 = idlist2.size
        val overlapLen = idlist1.intersect(idlist2).size
        if (overlapLen==len2){
          label = label2
          res = true
        }else if(overlapLen==len1){
          res = true
        }
        (label, res)
      }
    }).filter(x=>x._2)
    duplicateComs.foreach(_ => Unit)
    val overlapCommunities = communities.leftOuterJoin(duplicateComs).filter(x=>x._2._2.isEmpty).mapPartitions(iter =>
      for((label, (nodes,signal)) <- iter) yield (label, nodes.toArray))

    val verticesValidLabels = overlapCommunities.flatMap(vertex => {
      val (label, vids) = vertex
      var result =  new mutable.ArrayBuffer[(Long, Array[Long])]
      vids.foreach(vid =>  result += ((vid,Array(label))))
      result
    }).reduceByKey(_++_)
    verticesValidLabels.foreach(_ => Unit)
    val resGraph = graph.outerJoinVertices(verticesValidLabels)((vid, vData, validLabelOption) => {
      val validLabel = validLabelOption.orNull
      if (validLabel != null) {
        val newLabels = scala.collection.mutable.HashMap[Long, Double]()
        validLabel.foreach(label => {
          if(vData.estimateLabel.contains(label)) newLabels(label) = vData.estimateLabel(label)
        }
        )
        vData.estimateLabel  = newLabels.toMap
      }
      vData
    })
    resGraph.vertices.foreach(_ => Unit)
    (overlapCommunities, resGraph.mapEdges(e => e.attr._1))
    (overlapCommunities, graph.mapEdges(e => e.attr._1))
  }

   */
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
    //    val detectCommunities = sc.parallelize(Array(Array(0.toLong,1.toLong,5.toLong),
    //      Array(1.toLong,2.toLong,3.toLong,4.toLong,7.toLong,8.toLong)))
    //    val realCommunities = sc.parallelize(Array(Array(0.toLong,5.toLong),
    //      Array(0.toLong,1.toLong,2.toLong,3.toLong,4.toLong,7.toLong,8.toLong)))

    val console = new OverlapNMI()

//        println(s"nmi.lfk: ${console.NMI_LFK(a, a)}")
//        println(s"nmi.max: ${console.NMI_max(a, a)}")

//        println(console.pureRate(sc,detectCommunities,vidLabels))
  }


}