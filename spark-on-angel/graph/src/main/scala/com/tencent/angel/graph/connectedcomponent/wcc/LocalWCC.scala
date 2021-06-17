package com.tencent.angel.graph.connectedcomponent.wcc

import com.tencent.angel.graph.utils.collection.OpenHashMap
import edu.princeton.cs.algs4.UF


object LocalWCC {
  def process(edge: Array[(Long, Long)]): Map[Long, Long] = {
    val nodes = edge.flatMap { case (src, dst) => Iterator(src, dst) }.distinct
    val global2local = new OpenHashMap[Long, Int](nodes.length)
    nodes.indices.foreach { i => global2local(nodes(i)) = i }
    
    //Union-Find
    val uf = new UF(nodes.length)
    edge.foreach { case (src, dst) =>
      uf.union(global2local(src), global2local(dst))
    }
    
    // local2global
    val maps: Map[Long, Long] = global2local.map { case (global, local) =>
      global -> nodes(uf.find(local))
    }(collection.breakOut)
    
    println(s"[wcc-local]localCC finished")
    maps
  }
}
