package com.tencent.angel.spark.ml.ann.index

import com.tencent.angel.spark.ml.ann._
import com.tencent.angel.spark.ml.ann.index.HnswIndex.{HnswIndexBuilder, IdWithDistance, Node}
import com.tencent.angel.spark.ml.util.Murmur3
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import javax.naming.SizeLimitExceededException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HnswIndex extends IndexGraph with Serializable {
  
  var M: Int = 10
  var efConstruction: Int = 200
  var ef: Int = 10
  var mL: Double = 1 / Math.log(10)
  var maxM0: Int = 20
  var maxM: Int = 10
  
  var maxItemCount: Int = Integer.MAX_VALUE
  
  var lookup: Object2IntOpenHashMap[Long] = new Object2IntOpenHashMap[Long]()
  var nodes: ArrayBuffer[Node] = new ArrayBuffer[Node]()
  var NO_NODE_ID: Int = -1
  var enterPoint: Node = _
  var nodeCount: Int = 0
  
  def hnswIndexBuilder(M: Int, efConstruction: Int, ef: Int,
                       distanceFunction: DistanceFunction = new DistanceFunctions.FloatL2Distance,
                       distanceComparator: Ordering[Float] = Ordering.fromLessThan[Float]((a, b) => a < b),
                       maxItemCount: Int = Integer.MAX_VALUE): Unit = {
    this.hnswIndexBuilder(M, efConstruction, ef, M, M * 2, 1 / Math.log(M),
      distanceFunction, distanceComparator, maxItemCount)
  }
  
  def hnswIndexBuilder(M: Int, efConstruction: Int, ef: Int,
                       maxM: Int, maxM0: Int, mL: Double,
                       distanceFunction: DistanceFunction,
                       distanceComparator: Ordering[Float],
                       maxItemCount: Int): Unit = {
    this.M = M
    this.maxM = maxM
    this.maxM0 = maxM0
    this.mL = mL
    this.efConstruction = Math.max(efConstruction, M)
    this.ef = ef
    this.distanceFunction = distanceFunction
    this.distanceComparator = distanceComparator
    this.maxItemCount = maxItemCount
  }
  
  def hnswIndexBuilder(builder: HnswIndexBuilder): Unit = {
    hnswIndexBuilder(builder.M, builder.efConstruction, builder.ef,
      builder.maxM, builder.maxM0, builder.mL, builder.distanceFunction,
      builder.distanceComparator, builder.maxItemCount)
  }
  
  override def add(item: Item): Boolean = {
    val randomLevel = this.assignLevel(item.getId, this.mL)
    
    val connections: Array[IntArrayList] = new Array[IntArrayList](randomLevel + 1)
    
    for (level <- 0 to randomLevel) {
      val levelM = randomLevel match {
        case 0 => this.maxM0
        case _ => this.maxM
      }
      connections(level) = new IntArrayList(levelM)
    }
    val existingNodeId = lookup.getOrDefault(item.getId, NO_NODE_ID)
    
    if (existingNodeId != NO_NODE_ID) {
      println(s"item has been inserted, multiple tries to insert a same node..")
      false
    }
    else {
      if (this.nodeCount >= this.maxItemCount) {
        throw new SizeLimitExceededException("The number of elements exceeds the specified limit.")
      }
      
      val newNodeId = nodeCount
      nodeCount += 1
      
      val newNode: Node = new Node(newNodeId, item, connections)
      
      nodes += newNode
      assert(nodes.length == nodeCount, "length of nodes array != count of nodes")
      
      lookup.put(item.getId, newNodeId)
      
      var currNode = this.enterPoint
      if (currNode != null) {
        if (newNode.maxLevel < enterPoint.maxLevel) {
          var curDis = distanceFunction.distance(item.getVector, currNode.item.getVector)
          
          for (activeLevel <- (randomLevel to enterPoint.maxLevel).reverse) {
            var changed = true
            while (changed) {
              changed = false
              val candidateConnections = currNode.connections(activeLevel)
              
              for (i <- 0 until candidateConnections.size()) {
                val candidateId = candidateConnections.getInt(i)
                val candidateNode = nodes(candidateId)
                
                val candidateDistance = distanceFunction.distance(item.getVector, candidateNode.item.getVector)
                if (lt(candidateDistance, curDis)) {
                  curDis = candidateDistance
                  currNode = candidateNode
                  changed = true
                }
              }
            }
          }
        }
        for (level <- (0 to Math.min(randomLevel, enterPoint.maxLevel)).reverse) {
          val topCandidates = searchLayer(newNode.item.getVector, this.enterPoint, this.efConstruction, level)
          connectNewNodeWithCandidates(newNode, topCandidates, level)
        }
      }
      
      if (this.enterPoint == null || newNode.maxLevel > this.enterPoint.maxLevel) {
        if (this.enterPoint == null) {
          println(s"enter point is null, init to ${newNode.item.id}")
        }
        else {
          println(s"enter point changes from ${this.enterPoint.item.id} to ${newNode.item.id}")
        }
        this.enterPoint = newNode
      }
      true
    }
  }
  
  override def addAll(item: Array[Item], listener: ProgressListener, progressUpdateInterval: Int): Unit = {
    assert(item.length > 0, s"add 0 items to graph, please check for items")
    super.addAll(item, listener, progressUpdateInterval)
  }
  
  //todo any optimization
  override def findNearest(vector: Array[Float], k: Int): Seq[SearchResult] = {
    
    assert(this.enterPoint != null, s"enter point is null, check graph...")
    
    implicit val ord: Ordering[IdWithDistance] = Ordering.by(_.distance)
    val candidates = new mutable.PriorityQueue[IdWithDistance]()(ord.reverse)
    val candidatesSet = new mutable.HashSet[Int]()
    
    var curEp = this.enterPoint
    val maxLayer = curEp.maxLevel
    for (layer <- (1 to maxLayer).reverse) {
      val layerQueue = searchLayer(vector, curEp, 1, layer)
      while (layerQueue.nonEmpty) {
        val pair = layerQueue.dequeue()
        if (!candidatesSet.contains(pair.id)) {
          candidatesSet.add(pair.id)
          candidates.enqueue(pair)
        }
      }
      
      curEp = nodes(candidates.head.id)
    }
    
    val bottomLayerQueue = searchLayer(vector, curEp, Math.max(this.ef, k), 0)
    
    while (bottomLayerQueue.nonEmpty) {
      val pair = bottomLayerQueue.dequeue()
      if (!candidatesSet.contains(pair.id)) {
        candidatesSet.add(pair.id)
        candidates.enqueue(pair)
      }
    }
    val results = new ArrayBuffer[SearchResult]()
    
    while (candidates.nonEmpty && results.length < k) {
      val pair = candidates.dequeue()
      results += new SearchResult(nodes(pair.id).item, pair.distance)
    }
    results
  }
  
  private def searchLayer(newVector: Array[Float],
                          enterPoint: Node,
                          topK: Int,
                          layer: Int): mutable.PriorityQueue[IdWithDistance] = {
    val visitedBitSet = new mutable.BitSet()
    
    implicit val ord: Ordering[IdWithDistance] = Ordering.by(_.distance)
    val topCandidates = new mutable.PriorityQueue[IdWithDistance]()(ord)
    val candidateSet = new mutable.PriorityQueue[IdWithDistance]()(ord.reverse)
    
    val distance: Float = distanceFunction.distance(newVector, enterPoint.item.getVector)
    val pair = new IdWithDistance(enterPoint.id, distance)
    topCandidates.enqueue(pair)
    var lowerbound = distance
    candidateSet.enqueue(pair)
    
    visitedBitSet.add(enterPoint.id)
    
    var break = false
    while (candidateSet.nonEmpty && !break) {
      val currPair = candidateSet.dequeue()
      
      //greater than lowerbound
      if (gt(currPair.distance, lowerbound)) {
        break = true
      }
      else {
        val node = nodes(currPair.id)
        val candidates = node.connections(layer)
        
        for (i <- 0 until candidates.size()) {
          val candidateId = candidates.getInt(i)
          if (!visitedBitSet.contains(candidateId)) {
            visitedBitSet.add(candidateId)
            
            val candidateNode = nodes(candidateId)
            
            val candidateDistance = this.distanceFunction.distance(newVector, candidateNode.item.getVector)
            
            if (topCandidates.size < topK || gt(lowerbound, candidateDistance)) {
              val candidatePair = new IdWithDistance(candidateId, candidateDistance)
              
              candidateSet.enqueue(candidatePair)
              
              topCandidates.enqueue(candidatePair)
              
              if (topCandidates.size > topK) {
                topCandidates.dequeue()
              }
              
              if(topCandidates.nonEmpty) {
                lowerbound = topCandidates.head.distance
              }
            }
          }
        }
      }
    }
    topCandidates
  }
  
  private def connectNewNodeWithCandidates(newNode: Node,
                                           topCandidates: mutable.PriorityQueue[IdWithDistance],
                                           layer: Int): Unit = {
    val bestN = layer match {
      case 0 => this.maxM0
      case _ => this.maxM
    }
    
    val newNodeId = newNode.id
    val newItemVector = newNode.item.getVector
    val newItemConnections = newNode.connections(layer)
    
    this.selectNeighborsHeuristic(newNode, topCandidates, this.M, layer)
    
    while (topCandidates.nonEmpty) {
      val seletedNbrId = topCandidates.dequeue().id
      
      newItemConnections.add(seletedNbrId)
      
      // check if len(nbr connections) is too large
      val nbrNode = nodes(seletedNbrId)
      val nbrConnectionsAtThisLayer = nbrNode.connections(layer)
      
      if (nbrConnectionsAtThisLayer.size() < bestN) {
        nbrConnectionsAtThisLayer.add(newNodeId)
      }
      else {
        val nbrDis = distanceFunction.distance(newItemVector, nbrNode.item.getVector)
        val nbrVector = nbrNode.item.getVector
        
        implicit val ord: Ordering[IdWithDistance] = Ordering.by(_.distance)
        val candidates = new mutable.PriorityQueue[IdWithDistance]()(ord)
        candidates.enqueue(new IdWithDistance(newNodeId, nbrDis))
        
        for (i <- 0 until nbrConnectionsAtThisLayer.size()) {
          val id = nbrConnectionsAtThisLayer.getInt(i)
          val dis = distanceFunction.distance(nbrVector, nodes(id).item.getVector)
          candidates.enqueue(new IdWithDistance(id, dis))
        }
        
        selectNeighborsHeuristic(newNode, candidates, bestN, layer)
        
        nbrConnectionsAtThisLayer.clear()
        
        while (candidates.nonEmpty) {
          nbrConnectionsAtThisLayer.add(candidates.dequeue().id)
        }
      }
    }
  }
  
  private def selectNeighborsHeuristic(newNode: Node,
                                       topCandidates: mutable.PriorityQueue[IdWithDistance],
                                       topK: Int,
                                       layer: Int,
                                       extendCandidates: Boolean = false,
                                       keepPrunedConnections: Boolean = false): Unit = {
    
    // extends candidates with neighbors, useful when graph is dense
    if (extendCandidates) {
      val extendSet = new mutable.HashSet[Int]()
      while (topCandidates.nonEmpty) {
        val pair = topCandidates.dequeue()
        extendSet.add(pair.id)
        for (id <- nodes(pair.id).connections(layer).toIntArray()) {
          extendSet.add(id)
        }
      }
      val qVector = newNode.item.getVector
      extendSet.foreach{ idx =>
        val dis = distanceFunction.distance(nodes(idx).item.getVector, qVector)
        topCandidates.enqueue(new IdWithDistance(idx, dis))
      }
    }
    
    if (topCandidates.size >= topK) {
      val retArray = new ArrayBuffer[IdWithDistance]()
      
      implicit val ord: Ordering[IdWithDistance] = Ordering.by(_.distance)
      val workingQueue = new mutable.PriorityQueue[IdWithDistance]()(ord.reverse)
      val discardedQueue = new mutable.PriorityQueue[IdWithDistance]()(ord.reverse)
      
      while (topCandidates.nonEmpty) {
        workingQueue.enqueue(topCandidates.dequeue())
      }
      
      while (workingQueue.nonEmpty && retArray.length < topK) {
        val curPair = workingQueue.dequeue()
        val distToQuery = curPair.distance
        
        var heuristicAdd = true
        var break = false
        var idx = 0
        while (idx < retArray.length && !break) {
          val pair = retArray(idx)
          val curDis = distanceFunction.distance(
            nodes(curPair.id).item.getVector, nodes(pair.id).item.getVector)
          
          if (gt(distToQuery, curDis)) {
            heuristicAdd = false
            break = true
          }
          idx += 1
        }
        
        if (heuristicAdd) {
          retArray += curPair
        }
        else {
          discardedQueue.enqueue(curPair)
        }
      }
      
      if (keepPrunedConnections) {
        while (discardedQueue.nonEmpty && retArray.length < topK) {
          retArray += discardedQueue.dequeue()
        }
      }
      
      topCandidates ++= retArray
      
      retArray.clear()
      discardedQueue.clear()
      workingQueue.clear()
    }
  }
  
  def assignLevel(id: Long, lambda: Double): Int = {
    val hashCode = id.hashCode()
    val bytes: Array[Byte] = Array[Byte](
      (hashCode >> 24).toByte,
      (hashCode >> 16).toByte,
      (hashCode >> 8).toByte,
      hashCode.toByte)
    
    val random = Math.abs(Murmur3.hash32(bytes).toDouble / Integer.MAX_VALUE.toDouble)
    
    val r = -Math.log(random) * lambda
    
    r.toInt
  }
  
  //todo more nodes in connections taken. now only nbr in connections(0)
  override def findNeighbors(id: Long, k: Int): Seq[SearchResult] = {
    val results = new ArrayBuffer[SearchResult]()
    if (lookup.containsKey(id)) {
      implicit val ord: Ordering[IdWithDistance] = Ordering.by(_.distance)
      val candidates = new mutable.PriorityQueue[IdWithDistance]()(ord.reverse)
      
      val node = nodes(lookup.getInt(id))
      val nbrList = nodes(lookup.getInt(id)).connections(0)
      
      
      nbrList.toIntArray().foreach{ nbrId =>
        val nbrNode = nodes(nbrId)
        val dis = distanceFunction.distance(node.item.getVector, nbrNode.item.getVector)
        candidates.enqueue(new IdWithDistance(nbrId, dis))
      }
      
      while (candidates.nonEmpty && results.length < k) {
        val pair = candidates.dequeue()
        results += new SearchResult(nodes(pair.id).item, pair.distance)
      }
      results
    }
    else {
      results
    }
  }
  
  override def size: Int = {
    lookup.size()
  }
  
  override def get(id: Long): Option[Item] = {
    val nId = lookup.getOrDefault(id, NO_NODE_ID)
    if (nId == NO_NODE_ID) {
      Option.empty[Item]
    }
    else {
      Option.apply(nodes(nId).item)
    }
  }
  
  override def contains(id: Long): Boolean = {
    lookup.containsKey(id)
  }
  
  override def apply(id: Long): Item = {
    val ret = get(id)
    ret match {
      case Some(ret) => ret
      case _ => throw new NoSuchElementException
    }
  }
  
  override def save(path: String): Boolean = ???
  
  override def load(path: String): Boolean = ???
  
  private def lt(x: Float, y: Float): Boolean = {
    this.distanceComparator.compare(x, y) < 0
  }
  
  private def gt(x: Float, y: Float): Boolean = {
    this.distanceComparator.compare(x, y) > 0
  }
  
}

object HnswIndex {
  class Node(val id: Int,
             val item: Item,
             val connections: Array[IntArrayList]) extends Serializable {
    
    def maxLevel: Int = {
      this.connections.length - 1
    }
  }
  
  class IdWithDistance(val id: Int,
                       val distance: Float) extends Serializable {
    
  }
  
  class HnswIndexBuilder(var M: Int, var efConstruction: Int, var ef: Int,
                         var maxM: Int, var maxM0: Int, var mL: Double,
                         var distanceFunction: DistanceFunction = new DistanceFunctions.FloatL2Distance,
                         var distanceComparator: Ordering[Float] = Ordering.fromLessThan[Float]((a, b) => a < b),
                         var maxItemCount: Int = Integer.MAX_VALUE) extends Serializable {
    
    def this(M: Int, efConstruction: Int, ef: Int) = this(M, efConstruction, ef, M, M * 2, 1 / Math.log(M),
      new DistanceFunctions.FloatL2Distance, Ordering.fromLessThan[Float]((a, b) => a < b), Integer.MAX_VALUE)
    
    def setMaxM(maxM: Int): Unit = {
      this.maxM = maxM
    }
    
    def setMaxM0(maxM0: Int): Unit = {
      this.maxM0 = maxM0
    }
    
    def setML(mL: Double): Unit = {
      this.mL = mL
    }
    
    def setDistanceFunction(distanceFunction: DistanceFunction): Unit = {
      this.distanceFunction = distanceFunction
    }
    
    def setDistanceFunction(disFunc: String): Unit = {
      this.distanceFunction = DistanceFunctions.str2DistanceFunction(disFunc)
    }
    
    def setDistanceComparator(distanceComparator: Ordering[Float]): Unit = {
      this.distanceComparator = distanceComparator
    }
    
    def setMaxItemCount(maxItemCount: Int): Unit = {
      this.maxItemCount = maxItemCount
    }
    
  }
  
}
