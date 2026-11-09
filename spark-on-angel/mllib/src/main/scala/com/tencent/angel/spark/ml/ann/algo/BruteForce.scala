package com.tencent.angel.spark.ml.ann.algo

import com.tencent.angel.spark.ml.ann.ANNModel
import com.tencent.angel.spark.ml.ann.partition.BruteForcePartition
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}

class BruteForce(override val uid: String) extends ANNModel[BruteForcePartition] {
  
  def this() = this(Identifiable.randomUID("BruteForce"))
  
  def transform(dataset: Dataset[_]): DataFrame = {
    
    this.start(dataset)
    
    println(s"start make ann partitions")
    val timeMkAnnParts = System.currentTimeMillis()
    val indexedParts = data.mapPartitionsWithIndex{ (idx, iter) =>
      Iterator.single(BruteForcePartition.apply(idx, iter, $(disFunc)))
    }
    indexedParts.persist($(storageLevel))
    indexedParts.foreachPartition(_ => Unit)
    println(s"make ann partitions cost: ${(System.currentTimeMillis() - timeMkAnnParts) / 1000.0} s")
    
    this.setANNPartsWithSheets(indexedParts)
    this.processANNParts()
    
    this.saveKNNResults()
  }
}