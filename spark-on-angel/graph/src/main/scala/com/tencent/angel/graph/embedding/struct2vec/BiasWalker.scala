package com.tencent.angel.graph.embedding.struct2vec

import struct2vec.Alias_table.alias_sample

import scala.collection.mutable.ListBuffer
import scala.util.Random

class BiasWalker (srcNodesArray: Array[Long]) {

  private var idx2Node = srcNodesArray
  private var idx: Array[Int] = Array(idx2Node.length)
  for(i <- 0 to idx2Node.length) idx(i)=i

//  def simulate_walks(num_walks: Int,walk_length: Int,stay_prob: Double =0.3) ={
//
//
//  }

  def _simulate_walks(nodes:Array[Int],num_walks: Int,walk_length: Int,stay_prob: Double,
                      layers_adj:Array[Array[Int]],layers_accept:Array[Array[Float]],
                      layers_alias:Array[Array[Float]],gamma:Array[Array[Float]]) ={
    var walks = new Array[ListBuffer[Long]](num_walks)
    Random.shuffle(nodes)
    for( i <- 0 to num_walks ) {
      for(node <- nodes)
        walks(i) = exec_random_walk(layers_adj,layers_accept,layers_alias,node,walk_length,gamma,stay_prob)
    }
    walks
  }

  def exec_random_walk(graphs: Array[Array[Array[Int]]], layers_accept: Array[Array[Array[Float]]], layers_alias: Array[Array[Array[Int]]]
                       , node: Int, walk_length: Int, gamma: Array[Array[Float]], stay_prob: Double)={
    var Initlayer: Int = 0
    var layer: Int = 0
    var path: ListBuffer[Long] = ListBuffer(idx.length)
    path.append(idx2Node(node))

    //同一层级
    while (path.length < walk_length){
      var rand = Random.nextFloat()
      if (rand < stay_prob) {
        node = ChooseNeigbor(node,graphs,layers_alias,layers_accept,layer)
        path.append(idx2Node(node))
      }else{  //  不同层级
      var x = math.log(gamma(layer)(node))+1
      var p_moveup = (x / (x+1))  // 升层级的概率

      if((rand > p_moveup)&&(layer > Initlayer))  //降层级
         layer = layer - 1
      if ((graphs.contains(layer+1))&&(graphs(layer+1).contains(node))) //升层级
        layer = layer + 1
    }
    path
  }
  def ChooseNeigbor(node: Int, graphs: Array[Array[Array[Int]]], layers_alias: Array[Array[Array[Int]]],
                    layers_accept: Array[Array[Array[Float]]], layer: Int) ={
    val node_list = graphs(layer)(node)
    val idx = alias_sample(Random,layers_accept(layer)(node),layers_alias(layer)(node),1)
    //node_list(idx)
  }

  }
}
