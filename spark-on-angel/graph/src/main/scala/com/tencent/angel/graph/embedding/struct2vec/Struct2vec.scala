package com.tencent.angel.graph.embedding.struct2vec

import com.tencent.angel.graph.embedding.struct2vec.{Struct2vecGraphPartition, Struct2vecParams}
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.graph.utils.{GraphIO, Stats}
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import java.lang.Math.{abs, exp, log, max, min}
import scala.collection.{Seq, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Queue, Set}
import struct2vec.fastdtw.fastdtw
import struct2vec.fastdtwUtils.{EuclideanSpace, TimeSeriesElement, VectorValue}
import struct2vec.Alias_table.createAliasTable

import java.lang.Math.exp


class Struct2vec(params: Struct2vecParams ) {


  private val idx2Nodes : Array[Int] = Array()

  private var output: String = _

  def this() = this(Identifiable.randomUID("Struct2vec"))

  def setOutputDir(in: String): Unit = {
    output = in
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    //create origin edges RDD and data preprocessing

    val rawEdges = NeighborDataOps.loadEdgesWithWeight(dataset, params.srcNodeIdCol, params.dstNodeIdCol, params.weightCol, params.isWeighted, params.needReplicaEdge, true, false, false)
    rawEdges.repartition(params.partitionNum).persist(params.StorageLevel.DISK_ONLY)
    val (minId, maxId, numEdges) = Stats.summarizeWithWeight(rawEdges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${params.StorageLevel}")
    for(i <- 0 to maxId) idx2Nodes(i) = i

    val edges = rawEdges.map { case (src, dst, w) => (src, (dst, w)) }

    // calc alias table for each node
    val aliasTable = edges.groupByKey(params.partitionNum).map(x => (x._1, x._2.toArray.distinct))
      .mapPartitionsWithIndex { case (partId, iter) =>
        Alias_table.calcAliasTable(partId, iter)
      }

    //ps process;create ps nodes adjacency matrix
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create model
    val modelContext = new ModelContext(params.psPartitionNum, minId, maxId + 1, -1,
      "struct2vec", SparkContext.getOrCreate().hadoopConfiguration)

    //    val data = edges.map(_._2._1) // ps loadBalance by in degree
    val data = edges.flatMap(f => Iterator(f._1, f._2._1)) //拿出（src，neighbors）

    //val model = DeepWalkPSModel.fromMinMax(minId, maxId, data, $(psPartitionNum), useBalancePartition = $(useBalancePartition))
    val model = Struct2vecPSModel(modelContext, data, params.useBalancePartition, params.balancePartitionPercent)
    val degreed_list = compute_orderd_degreelist(data,params.max_num_layers )
    val degrees = create_vector(data)

    //push node adjacency list into ps matrix; create graph with （node，sample path）
    val graphOri = aliasTable.mapPartitionsWithIndex((index, adjTable) =>
      Iterator(Struct2vecGraphPartition.initPSMatrixAndNodePath(model, index, adjTable, params.batchSize))))

    graphOri.persist($(storageLevel))
    //trigger action
    graphOri.foreachPartition(_ => Unit)

    // checkpoint
    model.checkpoint()

    var epoch = 0
    while (epoch < $(epochNum)) {
      var graph = graphOri.map(x => x.deepClone())
      //sample paths with random walk
      var curIteration = 0
      var prev = graph
      val beginTime = System.currentTimeMillis()
      do {
        val beforeSample = System.currentTimeMillis()
        curIteration += 1
        graph = prev.map(_.process(model, curIteration))
        graph.persist($(storageLevel))
        graph.count()
        prev.unpersist(true)
        prev = graph
        var sampleTime = (System.currentTimeMillis() - beforeSample)
        println(s"epoch $epoch, iter $curIteration, sampleTime: $sampleTime")
      } while (curIteration < $(walkLength) - 1)


      val EndTime = (System.currentTimeMillis() - beginTime)
      println(s"epoch $epoch, Struct2vecWithWeight all sampleTime: $EndTime")

      val temp = graph.flatMap(_.save())
      println(s"epoch $epoch, num path: ${temp.count()}")
      println(s"epoch $epoch, num invalid path: ${
        temp.filter(_.length != ${
          walkLength
        }).count()
      }")
      val tempRe = dataset.sparkSession.createDataFrame(temp.map(x => Row(x.mkString(" "))), transformSchema(dataset.schema))
      if (epoch == 0) {
        GraphIO.save(tempRe, output)
      }

      else {
        GraphIO.appendSave(tempRe, output)
      }
      println(s"epoch $epoch, saved results to $output")
      epoch += 1
      graph.unpersist()
    }

    val t = SparkContext.getOrCreate().parallelize(List("1", "2"), 1)
    dataset.sparkSession.createDataFrame(t.map(x => Row(x)), transformSchema(dataset.schema))
  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("path", StringType, nullable = false)))
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)



  def compute_orderd_degreelist(graph_adj:Iterator[(Long,Array[Long])],max_num_layers:Int):ArrayBuffer[Array[Array[(Long,Long)]]]={
    val order_list: ArrayBuffer[Array[Array[(Long,Long)]]] = ArrayBuffer()
    graph_adj.foreach { f =>
      order_list.append(get_orderd_degreelist(f._1.toInt,f._2,max_num_layers))
    }
    order_list  //[level,[node,[order,(degree,count)]]
    }

  //计算有序的度列表,获取单个节点的有序度序列 graph:Array[(Long, Array[Long])]
  def get_orderd_degreelist(root:Int,neighbors:Array[Long],max_num_layers: Int): Array[Array[(Long,Long)]] = {
    var ordered_degree_sequence_dict :Array[Array[(Long,Long)]] = Array()
    var visited :ArrayBuffer[Boolean] = ArrayBuffer.fill(idx2Nodes.length)(false)
    var queue :mutable.Queue[Int] = mutable.Queue()
    var degree_list:ArrayBuffer[Long] = ArrayBuffer()
    var orderd_degree_list : Array[(Long,Long)] = Array()

    var level = 0
    visited(root) = true


    while(queue.length>0 && level < max_num_layers ){
      var count = queue.length
      //opt1_method
      while(queue.length>0){
        var top = queue.dequeue().toInt
        var node = idx2Nodes(top)
        var degree = neighbors.length

        degree_list(degree) += 1  // count node freq

        for(nei <- neighbors){
          var nei_index = idx2Nodes(nei.toInt)
          if( !visited(nei_index)){
            visited(nei_index) = true
            queue.enqueue(nei_index)
          }
        }
        count-=1
      }
      for(degree <- degree_list;index <- 0 to degree_list.length) {
        orderd_degree_list(index) = (degree,degree_list(degree.toInt))
      }
      orderd_degree_list.sortBy(f => f._1)
      ordered_degree_sequence_dict(level) = orderd_degree_list
      level+=1
    }

    ordered_degree_sequence_dict
  }

  //


  //计算结构距离
  def compute_structural_distance(degreeList:ArrayBuffer[Array[Array[(Long,Long)]]],
                                  graph_adj:Iterator[(Long,Array[Long])],
                                  max_num_layers:Int,
                                  degrees:(ArrayBuffer[Long],Array[Long],Array[Long]),
                                  workers:Int=1,verbose:Int=0) = {

    if (params.opt2_reduce_sim_calc == true) {
      var degreeListSelected: Array[Array[Array[(Long,Long)]]] = Array()
      var vertices: Array[ArrayBuffer[Long]] = Array()

      val n_nodes = idx2Nodes.length

      graph_adj.foreach{ case (v, neighbors) =>
        var nbs = get_vertices(v,neighbors.length,degrees, n_nodes)
        vertices(v.toInt) = nbs  // store nbs
        degreeListSelected(v.toInt)=degreeList(v.toInt)  //store dist

        for(n <- nbs)
          degreeListSelected(n.toInt)=degreeList(n.toInt) //store dist of nbs
      }




    }else{
      var vertices: Array[Array[Int]] = Array()

      for(v <- idx2Nodes) {
        vertices(v) = for(vd <- idx2Nodes if vd > v ) yield vd
      }
    }
//      for(part_list in partition_dict(vertices,workers)){
//      var results = Parallel(workers)(delayed(cpmpute_dtw_dist)(part_list,degreelist,dist_func))
//     }
    var dtw_dist = compute_dtw_dist(graph_adj,degreeList)
    var structural_dist = convert_dtw_struc_dist(dtw_dist)
    structural_dist
    }


  def create_vector(graph:Iterator[(Long,Array[Long])] ) = {
    var degrees_sort :Set[Long] = Set()
    var vertices:ArrayBuffer[Long] = ArrayBuffer()

    graph.foreach{ case (node,neighbors) =>
      var degree = neighbors.length
      degrees_sort.add(degree)
      vertices.append(node)
    }
    var degrees_sorted = degrees_sort.toArray.sorted
    var l = degrees_sorted.length

    var degrees_before:Array[Long] = Array.fill(l)(0)
    var degrees_after:Array[Long] = Array.fill(l)(0)

    var index = 0
    for(degree<- degrees_sorted) {
      if (index > 0) {
        degrees_before(index) = degrees_sorted(index - 1)
      }
      if (index < (l - 1)) {
        degrees_after(index) =degrees_sorted(index + 1)
      }
      index += 1
    }
    (vertices,degrees_before,degrees_after)  // ( degree->数组索引,(node,before,after))
  }
  //确定度
  def verifyDegrees(degree_v_root:Int,degree_a:Long,degree_b:Long): Long ={
    if(degree_b == -1)
      degree_a
    else if (degree_a == -1)
      degree_b
    else if (abs(degree_b - degree_v_root) < abs(degree_a - degree_v_root))
      degree_b
    else
      degree_a
  }

  def judge_degree_one(before:Long):Long = {
    if(before==0) -1
    else before
  }

  def judge_degree_two(now:Long,degree_b:Long,before:Long,after:Long):Long = {
    if(degree_b==now) judge_degree_one(before)
    else judge_degree_one(after)
  }
  // opt2
  def get_vertices(v: Long,
                   degree_v: Int,
                   degrees: (ArrayBuffer[Long], Array[Long], Array[Long]),
                   n_nodes: Int): ArrayBuffer[Long] = {
    val nodes = degrees._1
    val before = degrees._2
    val after = degrees._3
    val a_vertices_selected = 2 * (log(n_nodes)/log(2))
    var vertices :ArrayBuffer[Long] = ArrayBuffer()
    try{
      var c_v = 0
      for(v2 <- nodes){
        if(v!=v2){
          vertices.append(v2)
          c_v += 1
          if(c_v > a_vertices_selected)
            return vertices// stop Iteration
        }
      }
      var degree_a = judge_degree_one(before(degree_v))
      var degree_b = judge_degree_one(after(degree_v))


      var degree_now = verifyDegrees(degree_v,degree_a,degree_b)

      //nearest vaild degree
      while (true){
        vertices.foreach(v2 =>{
          if(v!=v2){
            vertices.append(v2)
            c_v+=1
            if(c_v > a_vertices_selected)
              return vertices // stop Iteration
          }
        })
        degree_a = judge_degree_two(degree_now,degree_b,before(degree_b.toInt),after(degree_a.toInt))
        degree_b = judge_degree_two(degree_now,degree_b,before(degree_b.toInt),after(degree_a.toInt))

        if(degree_a == -1 & degree_b == -1)
          return vertices // stop Iteration

        degree_now = verifyDegrees(degree_v,degree_a,degree_b)
      }
    }
  vertices
  }
  //获得层级
  def get_layer_rep(pair_distance:Array[((Long,Long),ArrayBuffer[(Int,Double)])]) ={

    var layer_distances:Array[(Long,Long,Double)] = Array()  //(vx,vy,distance)
    var layer_adj:Array[(Long,Long,Long)] = Array()              // (layer,vx,vy)

    pair_distance.foreach{ case (v_pair,layer_distance) =>
      for((layer,distance)<- layer_distance){
        var vx = v_pair._1
        var vy = v_pair._2

        layer_distances(layer) = (vx,vy,distance)

        layer_adj(layer) = (layer,vx,vy)
        layer_adj(layer) = (layer,vy,vx)

      }
    }
  (layer_adj,layer_distances)
  }

  def change_layeradj(layers_adj:Array[(Int,Long,Long)]) = {
    layers_adj.map{case(layer,vx,vy) => (layer,(vx,vy))}.groupBy(_._1).map(x=>(x._1,x._2.map(x=>(x._2._2)))).toArray

  }
  def change_layer_distance(layers_distance:Array[(Long,Long,Double)]) ={
    layers_distance.map{case(vx,vy,distance) => ((vx,vy),distance)}.toArray
  }

  def search_distance(layers_distance:Array[((Long,Long),Double)],v:Tuple2[Long,Long]):Float = {
    layers_distance.foreach {
      case (v_pair, dist) =>
        if (v_pair == v) {
          return dist.toFloat
        }
    }
    0
  }

  //转移概率
  def get_transition_probs(layers_distance: Array[((Long, Long), Double)],
                           layers_adj: Array[(Int, Array[Long])]) = {

    var layers_alias: Array[Array[Int]] = Array()
    var layers_accept: Array[Array[Float]] = Array()
    var layers = 1
    val v_pair = layers_distance.map(f => f._1)
    var wd, w = 0.0
    var norm_weights: Array[ArrayBuffer[Float]] = Array()

    layers_adj.foreach { case (v, neighbors) =>

      var e_list: ArrayBuffer[Float] = ArrayBuffer()
      var sum_weight = 0.0


      for (nei <- neighbors) {
        if (v_pair.contains((v, nei))) {
          wd = search_distance(layers_distance, (v, nei))
        } else wd = search_distance(layers_distance, (nei, v))

        w = exp(-wd)
        e_list.append(w.toFloat)
        sum_weight += w
      }
      e_list = for (x <- e_list) yield {
        x / sum_weight
      }
      var e_list_w: Array[Float] = Array()
      for (i <- 0 to e_list.length) e_list_w(i) = e_list(i)
      norm_weights(v) = e_list

      var acp_alias = createAliasTable(e_list_w)
      layers_accept(layers) = acp_alias._1
      layers_alias(layers) = acp_alias._2

    }
    (layers_accept, layers_alias)
  }


  def cost(a: List[Int], b: List[Int]): Double = {
    val ep = 0.5
    val m = max(a(0), b(0)) + ep
    val mi = min(a(0), b(0)) + ep
    val result = ((m / mi) - 1)
    return result
  }

  def cost_min(a: List[Int], b: List[Int]): Double = {
    val ep = 0.5
    val m = max(a(0), b(0)) + ep
    val mi = min(a(0), b(0)) + ep
    val result = ((m / mi) - 1) * min(a(1), b(1))
    return result
  }

  def cost_max(a: List[Int], b: List[Int]): Double = {
    val ep = 0.5
    val m = max(a(0), b(0)) + ep
    val mi = min(a(0), b(0)) + ep
    val result = ((m / mi) - 1) * max(a(1), b(1))
    return result
  }

  //dtw转换成结构距离  (layer,(v1,v2,dist))
  def convert_dtw_struc_dist(distances: ArrayBuffer[(Long, Long, Int, Double)], startLayer: Int = 1) = {
    var dist = distances.map { case (src, dist, layer, distance) => ((src, dist), (layer, distance)) }
    var pair = dist.groupBy(_._1).map(x => (x._1, x._2.map(x => (x._2._1, x._2._2)))).toArray //((v1,v2),ArrayBuffer(layer,distance))
    for ((vertices, layer_distance) <- pair) {
      var keys_layers = for ((layer, distance) <- layer_distance) yield layer
      keys_layers.sorted
      var startLayer = min(keys_layers.length, startLayer)
      for (layer <- 0 to startLayer)
        keys_layers.remove(0)

      for (layer <- keys_layers)
        layer_distance(layer) = (layer, layer_distance(layer)._2 + layer_distance(layer - 1)._2)
    }
    pair
  }


  //计算dtw距离
  def compute_dtw_dist(part_graph: Iterator[(Long, Array[Long])], degreeList: ArrayBuffer[Array[Array[(Long, Long)]]]) = {
    val pair_v = new ArrayBuffer[(Long, Long, Int, Double)]
    //    val layer_dist = new ArrayList[(Int,Double)]

    part_graph.foreach { case (v1, neighbors) =>
      var lists_v1 = degreeList(v1.toInt)
      neighbors.foreach(v2 => {
        var lists_v2 = degreeList(v2.toInt)
        var max_layer = min(lists_v1.length, lists_v2.length)

        for (layer <- 0 to max_layer) yield {
          var v1_degree_list = lists_v1(layer).map(f => f._1.toDouble).toSeq.map(v => TimeSeriesElement(Some(VectorValue(v))))
          var v2_degree_list = lists_v2(layer).map(f => f._1.toDouble).toSeq.map(v => TimeSeriesElement(Some(VectorValue(v))))
          var fdtw = new fastdtw(1, EuclideanSpace)
          //          var path = fdtw.evaluate(v1_degree_list,v2_degree_list).optimalPath
          var dist = fdtw.evaluate(v1_degree_list, v2_degree_list).optimalCost
          pair_v.append((v1, v2, layer, dist))
        }
      })
    }
    pair_v
  }


}

