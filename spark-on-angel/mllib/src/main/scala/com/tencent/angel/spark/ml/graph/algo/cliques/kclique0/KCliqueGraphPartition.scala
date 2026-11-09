package com.tencent.angel.spark.ml.graph.algo.cliques.kclique0

import com.tencent.angel.spark.ml.graph.data.{NeighborTable, PartitionId, VertexId}
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class KCliqueGraphPartition(index: PartitionId,
                            srcIds: Array[VertexId],
                            neighbors: Array[Array[VertexId]],
                            srcCliques: Array[Array[VertexId]]) extends Serializable {
  lazy val size: Int = srcIds.length

  def initPSNbrs(model: KCliquePSModel, batchSize: Int = Int.MaxValue): Unit = {

    println(s"init partition $index with neighbors")
    var startTS = System.currentTimeMillis()

    makeBatchIterator(batchSize).foreach { case (from, to) =>
      val srcToNbrs: Long2ObjectOpenHashMap[Array[VertexId]] = new Long2ObjectOpenHashMap[Array[VertexId]](batchSize)
      (from until to).foreach { idx =>
        srcToNbrs.put(srcIds(idx), neighbors(idx)) // ps只存储了比源点大的顶点id
      }
      model.initNeighbors(srcToNbrs)
      println(s"last batch cost ${System.currentTimeMillis() - startTS} ms")
    }
  }

  def makeBatchIterator(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0

    override def hasNext: Boolean = {
      index < KCliqueGraphPartition.this.size
    }

    override def next(): (Int, Int) = {
      val prevIndex = index
      index = index + batchSize
      (prevIndex, math.min(index, KCliqueGraphPartition.this.size))
    }
  }

  def process(model: KCliquePSModel, nK: Int, batchSize: Int = Int.MaxValue): (Long, KCliqueGraphPartition) = {
    var cliqueCnt = 0L
    val newCliqueRec = new Array[Array[Long]](srcIds.length)

    var batchStratTime = System.currentTimeMillis()

    makeBatchIterator(batchSize).foreach { case (from, to) =>

      // nodes which need pull from ps
      val pullNodes: mutable.HashSet[Long] = mutable.HashSet[Long]()
      (from until to).foreach { pos =>
        pullNodes ++= neighbors(pos)
      }

      println(s"process ${to - from} nodes, pull ${pullNodes.size} neighbor tables from ps")
      val psNbrTable = model.readMsgs(pullNodes.toArray)

      var timeCnt1 = 0L
      var timeCnt2 = 0L
      var timeCnt3 = 0L

      (from until to).foreach { pos =>

        assert(srcCliques(pos).length % (nK - 2) == 0, s"nK == $nK, lenghth: ${srcCliques(pos).length}, id : ${srcIds(pos)}")

        val numClqs = srcCliques(pos).length / (nK - 2)

        val clqInPos = ArrayBuffer[Long]()

        // nodes bigger than src
        val pickNbrs = neighbors(pos).filter(_ > srcIds(pos))
        for (nbr <- pickNbrs) {
          (0 until numClqs).foreach { clqIdx =>

            val timeStep1 = System.currentTimeMillis()

            //取一个clique
            val clq = mutable.HashSet[Long](srcIds(pos))
            val cStart = clqIdx * (nK - 2) // nK-2 = 1
          val cEnd = (clqIdx + 1) * (nK - 2) //nK-2 = 1
            (cStart until cEnd).foreach { idx => // 取clique的start->end的顶点 + src
              clq.add(srcCliques(pos)(idx))
            }

            assert(clq.size == nK - 1) //第一次：nK-1 = 2

            // keep clique info stored in min id nodes
            // expand cliques each iter with bigger id node
            val maxId = clq.max

            val timeStep2 = System.currentTimeMillis()
            var t1 = 0L
            var t2 = 0L
            if (nbr > maxId) {
              val nbrSuper = psNbrTable.get(nbr)
              t1 = System.currentTimeMillis()
              var cntC = 0
              for (nc <- nbrSuper) {
                if (clq.contains(nc)) { // 共同好友
                  cntC += 1
                }
              }
              t2 = System.currentTimeMillis()

              if (clq.size == cntC) {
                cliqueCnt += 1
                clqInPos += nbr
                clq.remove(srcIds(pos))
                clqInPos ++= clq.toArray //新的clique
                // println(s"clqInPos size: ${clqInPos.length}, clq length: ${clq.size}")
              }

            }
            clq.clear()

            val timeStep3 = System.currentTimeMillis()

            timeCnt1 += timeStep2 - timeStep1
            timeCnt2 += timeStep3 - timeStep2
            timeCnt3 += t2 - t1

          }
        }

        newCliqueRec(pos) = clqInPos.toArray
      }
      var time3 = System.currentTimeMillis()

      println(s"$timeCnt1---$timeCnt2---$timeCnt3")

      // println(s"${time2 - time1}---${time3 - time2}")
      println(s"partition $index, last batch costs ${System.currentTimeMillis() - batchStratTime} ms")
      batchStratTime = System.currentTimeMillis()
    }

    (cliqueCnt, new KCliqueGraphPartition(index, srcIds, neighbors, newCliqueRec))
  }

  def testPS(psModel: KCliquePSModel, num: Int = 10): Boolean = {
    var correct = true
    assert(num <= size, s"the size of partition $size < $num")
    // check neighbor table
    for (i <- 0 until num - 1) {
      val srcId1 = srcIds(i)
      val srcId2 = srcIds(i + 1)
      val neighbors1 = neighbors(i)
      val neighbors2 = neighbors(i + 1)
      val trueNum = ArrayUtils.intersectCount(neighbors1, neighbors2)
      val fromPS = psModel.readMsgs(Array(srcId1, srcId2))
      val psNum = ArrayUtils.intersectCount(fromPS.get(srcId1), fromPS.get(srcId2))
      println(s"friends of $srcId1 = ${neighbors1.length} [RDD] ${fromPS.get(srcId1).length} [PS], " +
        s"friends of $srcId2 = ${neighbors2.length} [RDD] ${fromPS.get(srcId2).length} [PS], " +
        s"common friends = $trueNum [RDD] $psNum [PS]")
      if (correct && trueNum != psNum)
        correct = false
    }
    correct
  }

  def save(): (Array[Long], Array[Array[Long]]) = {
    (srcIds, srcCliques)
  }
}

object KCliqueGraphPartition {
  def apply(index: Int, data: Iterator[(Long, Iterable[Long])]): KCliqueGraphPartition = {
    val localSrcIds = ArrayBuffer[VertexId]()
    val localNbrs = ArrayBuffer[Array[VertexId]]()
    val localSrcClqs = ArrayBuffer[Array[VertexId]]()

    while (data.hasNext) {
      val (nd, nbr) = data.next()
      val nbrList = nbr.toArray.distinct
      localSrcIds += nd
      localNbrs += nbrList
      localSrcClqs += nbrList.filter(_ > nd) // 邻居id都大于原顶点id
    }

    new KCliqueGraphPartition(index,
      localSrcIds.toArray,
      localNbrs.toArray,
      localSrcClqs.toArray)
  }


  def fromNeighborTableRDD[ED: ClassTag](data: RDD[NeighborTable[ED]]): RDD[KCliqueGraphPartition] = {
    data.mapPartitionsWithIndex { case (partId, iter) =>
      val localSrcIds = ArrayBuffer[VertexId]()
      val localNeighbors = ArrayBuffer[Array[VertexId]]()
      val localSrcCliques = ArrayBuffer[Array[VertexId]]()

      iter.foreach { item =>
        localSrcIds += item.srcId
        localNeighbors += item.neighborIds
        localSrcCliques += item.neighborIds.filter(_ > item.srcId)
      }

      Iterator.single(
        new KCliqueGraphPartition(partId,
          localSrcIds.toArray,
          localNeighbors.toArray,
          localSrcCliques.toArray))
    }
  }
}
