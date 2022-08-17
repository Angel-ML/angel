package com.tencent.angel.graph.embedding.struct2vec

import breeze.linalg.BroadcastedColumns.broadcastOp
import breeze.linalg.BroadcastedRows.broadcastOp
import breeze.linalg.Vector.castFunc

import java.lang.Math.{abs, max, min}
import breeze.linalg.sum
import breeze.numerics.exp

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Queue, Set}
import scala.util.Random

class Struct2vec {

  private val graph_Nodes : List[Int] = List()
  private val idx2Nodes : List[Int] = List()
  private val Nodes2idx : List[Int] = List()
  private val idx : List[Int] = List.range(0,idx2Nodes.length,1)
  private val graph : List[Int] = List()
  private val embedding :Map[String,Int] = Map()

  val reuse :Boolean => _
  val output :String => _
  val opt1_reduce_len :Boolean => _
  val opt2_reduce_sim_calc :Boolean => _
  val opt3_num_layers: Int => _

//  def this() {
//    this(s"Node2Vec_${(new Random).nextInt()}")
//  }
//
//  def setOutputDir(in: String): Unit = {
//    output = in
//  }


  private def Init(
                    name: String, walk_length: Int, workers: Int, stay_prob: Float, opt1_reduce_len: Boolean,
                    opt2_reduce_sim_calc: Boolean, opt3_num_layers: Int, reuse : Boolean
                  ) = {
    //inital parmas
    val opt1_reduce_len :Boolean = opt1_reduce_len
    val opt2_reduce_sim_calc :Boolean = opt2_reduce_sim_calc
    val opt3_num_layers: Int = opt3_num_layers

  }

  private def create_context_graph(max_num_layers: Int, workers: Int): Unit = {

  }

  def compute_orderd_degreelist(max_num_layers: Int): Map[Int, Int] = {
    var degreeList : Map[Int, Int] = Map()
    var nodes = graph_Nodes

    for(node <- nodes){
      degreeList+=(node->get_order_degreelist_node(node,max_num_layers))
    }
    return degreeList
  }

  def get_order_degreelist_node(root: Int, max_num_layers: Int=0):Int = {

    if (max_num_layers == 0) {max_num_layers = Double.PositiveInfinity }

    var order_degree_sequence_dict:Map[Int,Int] = Map()

    var visited:ListBuffer[Boolean] =  ListBuffer.fill(graph_Nodes.length)(false)

    val queue : Queue[Int] = Queue()
    var level =0

    // in queue
    queue += root
    visited(root) = true

    while(queue.length >0 && level <= max_num_layers){
      var count :Int = queue.length


      while(count > 0){
        var top = queue.dequeue()
        var node =  idx2Nodes(top)
        var degree = graph(node)

        if (opt1_reduce_len == true){
          var degree_list : mutable.Map[Int,Int] = mutable.Map()
          degree_list(degree) = degree_list.get(degree) + 1
        }else{
          var degree_list : List[Int] = List()
          degree_list:+degree
        }



        for (nei <- graph(node)){
          val nei_idx = Nodes2idx(nei)
          if (visited(nei_idx) == false){
            visited(nei_idx) = true
            queue.enqueue(nei_idx)
          }
        }
        count-=1
      }

      var order_degree_list:ListBuffer[(Int,Int)] = ListBuffer()
      if(opt1_reduce_len == true){
        for((degree :Int,freq :Int)<- degree_list){
          order_degree_list.append((degree,freq))
        }
        order_degree_list.sortBy(_._1)

      }else{order_degree_list.sorted}

      order_degree_sequence_dict+=(level-> order_degree_list)
      level-=1
    }
  }

  def compute_structural_distance(max_num_layers:Int,workers:Int=1,verbose:Int=0): Unit = {

    if (opt1_reduce_len == true){
      var dist_func = cost_max()
    }else{
      var dist_func = cost()
    }

    var degreeList = compute_orderd_degreelist(max_num_layers)

    if (opt2_reduce_sim_calc == true){
      var degrees = create_vector()
      var degreeListSelected: Map[Int,Int] = Map()
      var vertices: Map[Int,Int] = Map()
      val n_nodes = idx.length
      for(v<-idx){
        var nbs = get_vertices(v,graph(idx2Nodes(v)),degrees,n_nodes)
        vertices+=(v-> nbs)
        degreeListSelected+=(v->degreeList(v))
        for(n<-nbs){
          degreeListSelected+=(n-> degreeList(n))
        }
      }

    }else{
      var vertices :Map[Int,List[Int]] = Map()
      for(v<-degreeList) {
        for(vd <- degreeList.keys){
          if (vd>v){vertices+=(v->vd)}
        }
     }
//      for(part_list in partition_dict(vertices,workers)){
//      var results = Parallel(workers)(delayed(cpmpute_dtw_dist)(part_list,degreelist,dist_func))
//     }
     // var dtw_dist = new Map[Map[]]()
//      val structural_dist = convert_dtw_struc_dist(dtw_dist)

    }
//    return structural_dist
  }

  def create_vector(){
    val degrees :Map[Int,Map[String,ListBuffer[Int]]]= Map()
    val degrees_sorted :Set[Int] = Set()
    val G = graph
    for(v<-idx){
      var degree :Int = G(idx2Nodes(v)).length
      degrees_sorted.add(degree)
//      if(degrees.contains(degree) == false){
//        degrees(degree) =
//        }
      degrees(degree)("vertices").append(v)
      degrees_sorted = degrees_sorted.toArray
      degrees_sorted.sorted

      var l = degrees_sorted.length
      var index = 0
      for(degree<- degree_sorted){
        if(index>0){
          degrees(degree)+=("before"-> degrees_sorted(index-1))
        }
        if(index<(l-1)){
          degrees(degree)+=("after"-> degrees_sorted(index+1))
        }
      }
    }
    return degrees
  }

  def get_layer_rep(pair_distance:Map[(Int,Int),Map[Int,(Int,Int)]]) {
    val layer_distances:Map[Int,Map[(Int,Int),(Int,Int)]]= Map()
    var layer_adj:Map[Int,Map[Int,ListBuffer[Int]]] = Map()

    for((v_pair,layer_dist)<- pair_distance ){
      for((layer,distance)<- layer_dist){
        var vx:Int = v_pair._1
        var vy:Int = v_pair._2

        layer_distances(layer)(vx,vy) -> (layer,distance)
        layer_adj(layer)(vx).append(vy)
        layer_adj(layer)(vy).append(vx)

      }
    }
    return layer_adj,layer_distances
  }

  def get_transition_probs(layers_adj:Map[Int,Map[Int,ListBuffer[Int]]],
                           layers_distances:Map[Int,Map[(Int,Int),(Int,Int)]]) {

    var layers_alias : Map[Int,Map[Int,Map[Int,List[Int]]]] = Map()
    var layers_accept :Map[Int,Map[Int,Map[Int,List[Int]]]] = Map()

    for(layer<- layers_adj.keys){
//      var neighbors :Map[Int,ListBuffer[Int]] = layers_adj(layer)
//      var layer_distances : Map[(Int,Int),(Int,Int)] = layers_distances(layer)
      var node_alias_dict :Map[Int,Map[Int,List[Int]]]  = Map()
      var node_accept_dict :Map[Int,Map[Int,List[Int]]]  = Map()
      var norm_weights :Map[Int,ListBuffer[Double]]  = Map()

      for((v,neighbors)<-layers_adj(layer)){
        var edge_list:ListBuffer[Double] = ListBuffer()
        var sum_weight :Double = 0.0

        for(n<- neighbors){
          if(layers_distances.contains((v,n))==true){
            edge_list.append(exp(layers_distances(layer)(v,n))
          }else{
            edge_list.append(exp(layers_distances(layer)(n,v))
          }
          sum_weight += exp(layers_distances(layer)(n,v))

          edge_list = for(x<-edge_list) yield x/sum_weight
          norm_weights+=(v->edge_list)
//          node_alias_dict+=(v->create_alias_table(edge_list))
//          node_accept_dict+=(v->create_alias_table(edge_list))
        }
        layers_alias+=(layer->node_alias_dict)
        layers_accept+=(layer->node_accept_dict)
      }
    }
    return layers_accept , layers_alias
  }

  def cost(a:List[Int],b:List[Int]):Double ={
    val ep=0.5
    val m = max(a(0),b(0))+ep
    val mi = min(a(0),b(0))+ep
    val result = ((m/mi)-1)
    return result
  }

  def cost_min(a:List[Int],b:List[Int]): Double ={
    val ep=0.5
    val m = max(a(0),b(0))+ep
    val mi = min(a(0),b(0))+ep
    val result = ((m/mi)-1) * min(a(1),b(1))
    return result
  }

  def cost_max(a:List[Int],b:List[Int]):Double={
    val ep=0.5
    val m = max(a(0),b(0))+ep
    val mi = min(a(0),b(0))+ep
    val result = ((m/mi)-1)*max(a(1),b(1))
    return result
  }

  def convert_dtw_struc_dist(distances:Map[Map[Int,List[(Int,Int)]],Map[Int,List[Int]]],startLayer:Int=1) = {
    for((vertices , layers)<-distances){
      var keys_layers = layers.keys.toList
      var startLayer:Int = min(keys_layers.length,startLayer)

      for(layer <- 0 to startLayer)  {keys_layers.remove(0)}
      for(layer <- keys_layers)   {layers(layer)+=layers(layer - 1)}
    }
    return distances
  }

  def verifyDegrees(degree:Int,degree_v_root:Int,degree_a:Int,degree_b:Int): Int ={
    var degree_now :Int = 0
    if(degree_b == -1){
      var degree_now:Int = degree_a
    } else if (degree_a == -1) {
      var degree_now :Int= degree_b
    } else if (abs(degree_b - degree_v_root) < abs(degree_a - degree_v_root)){
      var degree_now :Int= degree_b
    }else{
      var degree_now :Int= degree_a
    }
    return degree_now
  }

  def compute_dtw_dist(part_list:List[(Int,List[Int])],degreeList:Map[Int,List[Int]],dist_func:String){
    var dtw_dist :Map[(Int,Int),Int] = Map()
    for((v1,nbs)<- part_list){
      var lists_v1 = degreeList(v1)
      for(v2<-nbs){
        var lists_v2 = degreeList(v2)
        var max_layer = min(lists_v1.length,lists_v2.length)
        for(layer<- 0 to max_layer){
            var dist , path = fastdtw(list_v1(layer),list_v2(layer),radius=1,dist=dist_func)
          dtw_dist((v1,v2))+=(layer->dist)
        }
      }
    }
     return dtw_dist
  }



}
