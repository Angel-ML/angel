package com.tencent.angel.graph.embedding.struc2vec.test

import breeze.stats.distributions.AliasTable
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.embedding.struc2vec.StructureSimilarity
import com.tencent.angel.graph.embedding.struc2vec.test.Struc2Vec.{addDstAndWeights, addSrcAndLayer, calcAverageLayerWeights, calcCrossLayerWeights, sumLayerWeights,process}
import com.tencent.angel.graph.utils.{GraphIO, Stats}
import com.tencent.angel.graph.utils.params.{HasArrayBoundsPath, HasBalancePartitionPercent, HasBatchSize, HasDstNodeIdCol, HasEpochNum, HasIsWeighted, HasMaxIteration, HasNeedReplicaEdge, HasOutputCoreIdCol, HasOutputNodeIdCol, HasPSPartitionNum, HasPartitionNum, HasSrcNodeIdCol, HasStorageLevel, HasUseBalancePartition, HasUseEdgeBalancePartition, HasWalkLength, HasWeightCol}
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.{SPARK_BRANCH, SparkContext}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}
import org.apache.spark.storage.StorageLevel

import java.util.Random
import scala.collection.mutable.ArrayBuffer


class Struc2Vec(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasMaxIteration
  with HasBatchSize with HasArrayBoundsPath with HasIsWeighted with HasWeightCol with HasUseBalancePartition
  with HasNeedReplicaEdge with HasUseEdgeBalancePartition with HasWalkLength with HasEpochNum with HasBalancePartitionPercent {
  private var output: String = _

  def this() = this(Identifiable.randomUID("Struc2Vec"))

  def setOutputDir(in: String): Unit = {
    output = in
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    //create origin edges RDD and data preprocessing

    val rawEdges = NeighborDataOps.loadEdgesWithWeight(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(weightCol), $(isWeighted), $(needReplicaEdge), true, false, false)
    rawEdges.repartition($(partitionNum)).persist(StorageLevel.DISK_ONLY)
    val (minId, maxId, numEdges) = Stats.summarizeWithWeight(rawEdges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    val edges = rawEdges.map { case (src, dst, w) => (src, (dst, w)) }
    val rmWeight = rawEdges.map{case (src, dst, w) => (src, dst)}

    // create adjacency table
    val adjTable = rmWeight.groupByKey()

    // create adjacency matrix
    val len:Int = adjTable.count().toInt
    val adjArray = adjTable.collect()
    val adjMatrix: Array[Array[Int]] = Array.ofDim(len,len)

    // initialize adjMatrix
    for (i <- 0 until len; j<- 0 until len)
           if(i==j)
             adjMatrix(i)(j) = 0
           else
             adjMatrix(i)(j) = Int.MaxValue

    for(item <- adjArray){
      for(j<-item._2)
        adjMatrix(item._1.toInt)(j.toInt) = 1
    }

    val structureSimilarity = new StructureSimilarity(adjMatrix)

//    val diam = structureSimilarity.diam
//    println(s"diam = $diam")
//    val hop = structureSimilarity.hopCountResult
//    val degrees = structureSimilarity.degrees

    structureSimilarity.compute()

    // get the structure similarity distance network
    val simi = structureSimilarity.structSimi
    val diam = structureSimilarity.diam

    // create multilayer weighted graph RDD
    val multilayerRDD = adjTable.map(x => x._1.toInt).sortBy(x => x).flatMap(x => addSrcAndLayer(x,simi.length))
    val multilayerGraph = multilayerRDD.map(x => addDstAndWeights(x._1,x._2,simi)).filter(x=>x._2.length!=0)

    // create layer average weight array
    val layerAverageWeight = multilayerGraph.map(x=>sumLayerWeights(x)).groupByKey().map(x=>calcAverageLayerWeights(x)).sortByKey().collect()

    // create cross layer weight(level up weights)
    val crossLayerWeights = multilayerGraph.map(x=>calcCrossLayerWeights(x,layerAverageWeight))
    val crossArray = crossLayerWeights.collect()

    // create the alias table
    val aliasTable = multilayerGraph
      .mapPartitionsWithIndex { case (partId, iter) =>
        Struc2Vec.calcAliasTable(partId, iter)
      }
    val aliasArray = aliasTable.collect()

    val temp = aliasTable.filter(x => x._1==0&&x._2==0).collect()(0)
          println(s"temp:  ${temp._3.length}")


    // generate context for nodes
    // create the raw sample path rdd, (epochNum,src)
      val rawPathRDD = adjTable.map(x => x._1.toInt).sortBy(x => x).flatMap(x => addSrcAndLayer(x,$(epochNum)))
      val pathRDD = rawPathRDD.map{case (epochNum,src) => (src,epochNum) }.map(x => process(x,aliasArray,crossArray,$(walkLength),diam))

      val ele1 = multilayerGraph.count()
//    println(s"crosslen = ${crossArray.length}, ele1len = $ele1, sum = ${emp1.sum}, len = ${emp2.length}")

//    val item  = simi(1)(33)(0)
//    println(s"ele1 = $ele1   ele2 = $ele2    item = $item")

//    println("Layer1: ")
//    for (i <- 0 to simi(0).length - 1; j <- 0 to simi(0)(0).length - 1) {
//      print((i, j) + " = " + simi(0)(i)(j) + " ")
//    }
//    println("Layer5: ")
//    for (i <- 0 to simi(5).length - 1; j <- 0 to simi(0)(0).length - 1) {
//      print((i, j) + " = " + simi(5)(i)(j).isNaN+ " ")
//    }



//    val ele = aliasTable.collect()(203)
//    println(s"acc cnt = ${ele._4.length}")
//    for (i <- 0 to ele.length - 1) {
//          println("no."+ i + " = layer: " + ele(i)._1+ " src: "+ele(i)._2+"  dst: "+ele(i)._3(0)+"  wei: "+ele(i)._4(0)+" acc: "+ele(i)._5(0))
//        }

//    println("Degrees: ")
//    for (i <- 0 to degrees.length - 1) {
//      print("node"+ i + " = " + degrees(i)+ " ")
//    }

//    val fn =adjTable.take(17)(16)._2
//    val temp = adjArray(0)._2
//    val cnt = adjTable.count()
//    val ele = adjMatrix(32)(33)
//    println(s"get = $ele   count = $cnt")

    dataset.sparkSession.createDataFrame(pathRDD.map(x => Row(x._1,x._2,x._3)), transformSchema(dataset.schema))

  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("src",IntegerType, nullable = false),StructField("epochNum",IntegerType,nullable = false),StructField("path",StringType,nullable = false)))
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)



}

object Struc2Vec {
  def calcAliasTable(partId: Int, iter: Iterator[((Int,Int), Array[(Int, Double)])]): Iterator[(Int,Int, Array[Int], Array[Double], Array[Int])] = {
    iter.map { case ((layer,src), neighbors) =>
      val (events, weights) = neighbors.unzip
      val weightsSum = weights.sum
      val len = weights.length
      val areaRatio = weights.map(_ / weightsSum * len)
      val (accept, alias) = createAliasTable(areaRatio)
      (layer, src, events, accept, alias)
    }
  }

  def createAliasTable(areaRatio: Array[Double]): (Array[Double], Array[Int]) = {
    val len = areaRatio.length
    val small = ArrayBuffer[Int]()
    val large = ArrayBuffer[Int]()
    val accept = Array.fill(len)(0d)
    val alias = Array.fill(len)(0)

    for (idx <- areaRatio.indices) {
      // note that both accept and alias carries the indices of corresponding event indices, not the corresponding node!
      if (areaRatio(idx) < 1.0) small.append(idx) else large.append(idx)
    }
    while (small.nonEmpty && large.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      val largeIndex = large.remove(large.size - 1)
      accept(smallIndex) = areaRatio(smallIndex)
      alias(smallIndex) = largeIndex
      areaRatio(largeIndex) = areaRatio(largeIndex) - (1 - areaRatio(smallIndex))
      if (areaRatio(largeIndex) < 1.0) small.append(largeIndex) else large.append(largeIndex)
    }
    while (small.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      accept(smallIndex) = 1
    }

    while (large.nonEmpty) {
      val largeIndex = large.remove(large.size - 1)
      accept(largeIndex) = 1
    }
    (accept, alias)
  }

  def addSrcAndLayer(x: Int, layers: Int): Array[(Int, Int)] = {
    val temp = ArrayBuffer[(Int, Int)]()
    for (i: Int <- 0 until layers)
      temp.append((i, x))
    temp.toArray
  }

  def addDstAndWeights(layer:Int,src:Int,simi:Array[Array[Array[Double]]]):((Int,Int),Array[(Int,Double)])={
    val temp = ArrayBuffer[(Int,Double)]()
    for(dst<- 0 until simi(layer)(src).length){
      // here dst != src
      if(!simi(layer)(src)(dst).isNaN && dst!=src)
          temp.append((dst,Math.exp(-simi(layer)(src)(dst))))
    }
    ((layer,src),temp.toArray)
  }

  def sumLayerWeights(x:((Int,Int),Array[(Int,Double)])):(Int,(Double,Int))={
    val (dst,weights) = x._2.unzip
    val len = weights.length
    (x._1._1,(weights.sum,len))
  }

  def calcAverageLayerWeights(x:(Int,Iterable[(Double,Int)])): (Int,Double)={
    val (sum,cnt) = x._2.unzip
    (x._1,sum.sum/cnt.sum)
  }

  def calcCrossLayerWeights(x:((Int,Int),Array[(Int,Double)]),layerAverageWeights:Array[(Int,Double)]):((Int,Int),Double)={
    val (dst,w) = x._2.unzip
    val temp = w.filter(y => y>layerAverageWeights(x._1._1)._2)
    (x._1,Math.log(Math.E+temp.length))
  }

  def process(x: (Int, Int), aliasArray: Array[(Int, Int, Array[Int], Array[Double], Array[Int])], cross: Array[((Int, Int), Double)], walkLen: Int, diam:Int): (Int,Int,String) = {
    // ((Int,Int,Array[Int]))
    // initialize path
    val path = new ArrayBuffer[Int]()
    val (src, epochNum) = x
    var curLayer = 0
    path.append(src)

    // set the stay probability
    val stay = 0.5

    // start a random walk at node $src at layer 0 during $epochNUm
    while (path.length < walkLen) {
      val curNode: Int = path(path.length - 1)
      // decide whether stay in current layer
      val ifStay = new Random().nextDouble()
      if (ifStay < stay) {
        // stay in this layer
        val temp = aliasArray.filter(x => x._1 == curLayer && x._2 == curNode)(0)
        val events = temp._3
        val accepts = temp._4
        val alias = temp._5
        val index = new Random().nextInt(events.length)
        val isAccept = new Random().nextDouble()
        if(isAccept<accepts(index))
           path.append(events(index))
        else
          // note that alias carries the indices of corresponding event indices, not the corresponding node!
           path.append(events(alias(index)))
      }else{
        // change layer
        if(curLayer==0) {
          // layer 0 only levels up
          curLayer += 1
        }else if(curLayer == diam){
          // layer diam only lowers down
          curLayer -= 1
        }else{
          // decide up or down
          val up = cross.filter(x => x._1._1==curLayer&&x._1._2==curNode)(0)._2
          if(cross.filter(x => x._1._1==curLayer+1 && x._1._2==curNode).length!=0){
            val ifUp = new Random().nextDouble()
            if (ifUp < (up / (up + 1)))
              curLayer += 1
            else
              curLayer -= 1
          }else
            // not go to upper corresponding node with no neighbor
            curLayer -=1
        }
      }
    }
    (src,epochNum,path.toArray.mkString(" "))
  }
}