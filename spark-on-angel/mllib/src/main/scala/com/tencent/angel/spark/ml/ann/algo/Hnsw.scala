package com.tencent.angel.spark.ml.ann.algo

import com.tencent.angel.spark.ml.ann.ANNModel
import com.tencent.angel.spark.ml.ann.index.HnswIndex
import com.tencent.angel.spark.ml.ann.params._
import com.tencent.angel.spark.ml.ann.partition.HnswPartition
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}

class Hnsw(override val uid: String) extends ANNModel[HnswPartition]
  with HasM with HasEfConstruction with HasEf with HasMaxM with HasMaxM0 with HasML {
  
  def this() = this(Identifiable.randomUID("HNSW"))
  
  def transform(dataset: Dataset[_]): DataFrame = {
    
    this.start(dataset)
    
    println(s"start make ann partitions")
    val indexBuilder = new HnswIndex.HnswIndexBuilder($(M), $(efConstruction), $(ef), $(maxM), $(maxM0), $(mL))
    indexBuilder.setDistanceFunction($(disFunc))
    
    println(s"start make ann partitions and build graph")
    val timeBuildIndex = System.currentTimeMillis()
    
    val indexedGraphParts = data.mapPartitionsWithIndex{ (idx, iter) =>
      Iterator.single(HnswPartition.apply(idx, iter, indexBuilder))
    }.map(_.buildGraph())
    indexedGraphParts.persist($(storageLevel))
    indexedGraphParts.foreachPartition(_ => Unit)
    println(s"build index cost: ${(System.currentTimeMillis() - timeBuildIndex) / 1000.0} s")
    
    this.setANNPartsWithSheets(indexedGraphParts)
    this.processANNParts()
    
    this.saveKNNResults()
  }
}

