package com.tencent.angel.spark.ml.graph.algo.cliques.kcliqueV2

import com.tencent.angel.spark.ml.graph.data.{NeighborTable, PartitionId, VertexId}
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KCliqueGraphPartition(partitionID: PartitionId,
                            srcIds: Array[VertexId],
                            neighbors: Array[VertexId],
                            indptr: Array[Int],
                            batchSize: Int) extends Serializable {

  lazy val size: Int = srcIds.length
  
  lazy val numVertices: Long = srcIds.length
  
  lazy val numEdges: Long = neighbors.length
  
  // init msgs with 2-cliques
  def initMsgs(model: KCliquePSModel): Unit = {
    println(s"init partition $partitionID with 2-cliques")
    val startTS = System.currentTimeMillis()
    makeBatchIterator(batchSize).foreach{ case (from, to) =>
      println(s"last batch cost ${System.currentTimeMillis() - startTS} ms")
      val srcToCliques: Long2ObjectOpenHashMap[Array[VertexId]] = new Long2ObjectOpenHashMap[Array[VertexId]](batchSize)
      (from until to).foreach{ idx =>
        val largerNodes = ArrayBuffer[Long]()
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          if (neighbors(j) > srcIds(idx)) {
            largerNodes += neighbors(j)
          }
          j += 1
        }
        srcToCliques.put(srcIds(idx), largerNodes.toArray)
      }
      model.psPushClique(model.inMsgs, srcToCliques)
    }
  }
 
  def makeBatchIterator(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0
    
    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, KCliqueGraphPartition.this.size))
    }
    
    override def hasNext: Boolean = {
      index < KCliqueGraphPartition.this.size
    }
  }
  
  def testPS(psModel: KCliquePSModel, num: Int): Boolean = {
    var correct = true
    assert(num <= size, s"the size of partition $size < $num")
    // check neighbor table
    for (i <- 0 until num - 1) {
      val srcId1 = srcIds(i)
      val srcId2 = srcIds(i + 1)
      var j = indptr(i)
      val clk10 = ArrayBuffer[Long]()
      val clk20 = ArrayBuffer[Long]()
      while (j < indptr(i + 1)) {
        if (neighbors(j) > srcId1) {
          clk10 += neighbors(j)
        }
        j += 1
      }
      j = indptr(i + 1)
      while (j < indptr(i + 2)) {
        if (neighbors(j) > srcId2) {
          clk20 += neighbors(j)
        }
        j += 1
      }
      val clk1 = clk10.toArray
      val clk2 = clk20.toArray
      val trueNum = ArrayUtils.unionCount(clk1, clk2)
      val fromPS = psModel.readMsgs(Array(srcId1, srcId2))
      val psNum = ArrayUtils.unionCount(fromPS.get(srcId1), fromPS.get(srcId2))
      println(s"2-clique of $srcId1 = ${clk1.length} [RDD] ${fromPS.get(srcId1).length} [PS], " +
        s"2-clique of $srcId2 = ${clk2.length} [RDD] ${fromPS.get(srcId2).length} [PS], " +
        s"common friends = $trueNum [RDD] $psNum [PS]")
      if (correct && trueNum != psNum)
        correct = false
    }
    correct
  }
  
  // calculate kclique, return num of cliques
  def calculateKClique(psModel: KCliquePSModel, nK: Int): (Long, KCliqueGraphPartition) = {
    println(s"${nK}-clique calculation process start...")
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    var cliqueNum = 0L
    
    makeBatchIterator(batchSize).foreach { case (from, to) =>
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      (from until to).foreach { pos =>
        numSrcNodes += 1
        pullNodes += srcIds(pos)
        var j = indptr(pos)
        while (j < indptr(pos + 1)) {
          pullNodes += neighbors(j)
          j += 1
        }
      }
      val beforePullTs = System.currentTimeMillis()
      val psCliqueTable = psModel.readMsgs(pullNodes.toArray)
      
      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size
      
      println(s"partition $partitionID: process $numSrcNodes clique tables ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      
      val newCliqueTable: Long2ObjectOpenHashMap[Array[VertexId]] = new Long2ObjectOpenHashMap[Array[VertexId]]()
      
      (from until to).foreach { pos =>
        
        val srcNewClique = ArrayBuffer[Long]()
        // odd-cliques are stored in maximal nodes, even-clique in minimal nodes
        val srcNeighborsT = ArrayBuffer[Long]()
        if (nK % 2 == 1) {
          var j = indptr(pos)
          while (j < indptr(pos + 1)) {
            if (neighbors(j) < srcIds(pos)) {
              srcNeighborsT += neighbors(j)
            }
            j += 1
          }
        }
        else {
          var j = indptr(pos)
          while (j < indptr(pos + 1)) {
            if (neighbors(j) > srcIds(pos)) {
              srcNeighborsT += neighbors(j)
            }
            j += 1
          }
        }
        val srcNeighbors = srcNeighborsT.toArray
        val srcNbrSet = srcNeighbors.toSet
        
        srcNeighbors.foreach { dst =>
          // every (nK-2) nodes combined with dst makes a clique
          val dstCliques = psCliqueTable.get(dst)
          require(dstCliques.length % (nK-2) == 0, s"dst cliques should be divided by (nK-2)")
          val numCliques = dstCliques.length / (nK-2)
          
          // estimate whether all nodes in dst cliques are subset of srcNbrs
          (0 until numCliques).foreach { kCq =>
            val dCliqueNodes = collection.mutable.Set[Long]()
            dCliqueNodes.add(dst)
            val cStart = kCq * (nK - 2)
            val cEnd = (kCq + 1) * (nK - 2)
            (cStart until cEnd).foreach { idx =>
              dCliqueNodes.add(dstCliques(idx))
            }
            require(dCliqueNodes.size == (nK - 1), s"something went wrong causing error in clique nodes collection")
            // if dCliqueNodes subset of srcNbrSet, a nK-Clique is found
            if (dCliqueNodes.toSet.subsetOf(srcNbrSet)) {
              srcNewClique ++= dCliqueNodes.toArray
              cliqueNum += 1
            }
            dCliqueNodes.clear()
          }
        }
        newCliqueTable.put(srcIds(pos), srcNewClique.toArray)
      }
      psModel.writeMsgs(newCliqueTable)
      println(s"partition $partitionID: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
    }

    (cliqueNum, new KCliqueGraphPartition(partitionID, srcIds, neighbors, indptr, batchSize))
  }
  
  def countNodes(psModel: KCliquePSModel): Long = {
    val kCliques = psModel.readMsgs(srcIds)
    val keyArray = kCliques.keySet().toLongArray
    var cnt = 0L
    keyArray.foreach{ key =>
      // println(s"nodeId: $key contains: ${kCliques.get(key).mkString(",")}")
      if(kCliques.get(key).length > 0){
        cnt += 1
      }
    }
    cnt
  }
  
  def countCliquesNum(psModel: KCliquePSModel, nK: Int): Long = {
    val kCliques = psModel.readMsgs(srcIds)
    var numCliques = 0L
    srcIds.foreach{ key =>
      assert(kCliques.get(key).length % (nK - 1) == 0)
      numCliques += (kCliques.get(key).length / (nK - 1))
    }
    numCliques
  }
  
  def save(psModel: KCliquePSModel, kC: Int): Array[Array[Long]] = {
    val kCliques = psModel.readMsgs(srcIds)
    val keyArray = kCliques.keySet().toLongArray
    val retCliques = ArrayBuffer[Array[Long]]()
    keyArray.foreach { key =>
      val kClen = kCliques.get(key).length
      if (kClen > 0) {
        for (k <- 0 until (kClen / (kC-1))) {
          val tClique = ArrayBuffer[Long](key)
          for (i <- 0 until (kC-1)) {
            tClique += kCliques.get(key)(k * (kC-1) + i)
          }
          retCliques += tClique.toArray
        }
      }
    }
    retCliques.toArray
  }
  def save2(psModel: KCliquePSModel, kC: Int): (Array[Long], Array[Long]) = {
    val cliques = save(psModel, kC)
    val clkId = new ArrayBuffer[Long]()
    val nId = new ArrayBuffer[Long]()
    cliques.foreach { clk =>
      val minK = clk.min
      for (kd <- clk) {
        clkId += minK
        nId += kd
      }
    }
    (clkId.toArray, nId.toArray)
  }
  
}

object KCliqueGraphPartition {
  def apply(index: Int, data: Iterator[(Long, Iterable[Long])], batchSize: Int): KCliqueGraphPartition = {
    val srcIds = new LongArrayList()
    val neighbors = new LongArrayList()
    val indptr = new IntArrayList()
    
    indptr.add(0)
    
    while (data.hasNext) {
      val (src, ns) = data.next()
      srcIds.add(src)
      ns.toArray.distinct.foreach(n => neighbors.add(n))
      indptr.add(neighbors.size())
    }
    
    val srcIdsArray = srcIds.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    val indptrArray = indptr.toIntArray()
    
    new KCliqueGraphPartition(index, srcIdsArray, neighborsArray, indptrArray, batchSize)
  }
}

