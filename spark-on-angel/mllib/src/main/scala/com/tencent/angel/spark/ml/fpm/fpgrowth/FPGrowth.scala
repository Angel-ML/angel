package com.tencent.angel.spark.ml.fpm.fpgrowth

import com.tencent.angel.spark.ml.fpm.fpgrowth.FPGrowth.FrequentSet
import com.tencent.angel.spark.ml.graph.utils.params.{HasPartitionNum, HasStorageLevel}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{FloatParam, Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class FPGrowth(override val uid: String) extends Serializable
  with HasPartitionNum with HasStorageLevel {
  
  final val itemCol = new Param[String](this, "itemCol", "itemCol")
  final def setItemCol(colName: String): this.type = set(itemCol, colName)
  setDefault(itemCol, "items")
  
  final val minSupport = new FloatParam(this, "minSupport", "minSupport")
  final def setMinSupport(minSupp: Float): this.type = set(minSupport, minSupp)
  setDefault(minSupport, 0.7f)
  
  final val minConfidence = new FloatParam(this, "minConfidence", "minConfidence")
  final def setMinConfidene(cfd: Float): this.type = set(minConfidence, cfd)
  setDefault(minConfidence, 0.0f)
  
  def this() = this(Identifiable.randomUID("fpgrowth"))
  
  def fit(dataset: Dataset[_], id2Item: Option[Array[String]] = None): FPGrowthModel = {
    val ss = SparkSession.builder().getOrCreate()
  
    println("start fitting")
    
    val transactions = dataset.select($(itemCol)).rdd
      .filter(r => !r.anyNull)
      .map(r => r.getSeq[Int](0).toArray).repartition($(partitionNum))
    transactions.persist($(storageLevel))
    val count = transactions.count()
    val minCount = math.ceil($(minSupport) * count).toLong
  
    println(s"$count transactions collected")
    
    val partitioner = new HashPartitioner($(partitionNum))
    
    println("start detecting frequent items")
    val freqItemsCnt = findFrequentItems(minCount, transactions, partitioner)
    println(s"${freqItemsCnt.length} items with support larger than ${$(minSupport)}")
    println(s"samples: (item support)")
    freqItemsCnt.take(2).foreach( a => println(s"${a._1} ${a._2}"))
    
    println("start detecting frequent item sets")
    val freqItemsSets = findFrequentItemSets(minCount, transactions, freqItemsCnt.map(_._1), partitioner)
    freqItemsSets.persist($(storageLevel))
    val freqItemCnt = freqItemsSets.count()
    println(s"$freqItemCnt freqItemSet generated")
    transactions.unpersist()
    
    val retVal = freqItemsSets.map(f => Row(f.items, f.freq))
    retVal.persist($(storageLevel))
    retVal.count()
    freqItemsSets.unpersist()
    
    val schema = StructType(Seq(
      StructField("items", ArrayType(IntegerType), nullable = false),
      StructField("freq", LongType, nullable = false)))
    
    val itemSupport = freqItemsCnt.map{
      case (item, cnt) =>
        (item, cnt.toDouble / count)
    }.toMap
    
    println(schema)
    
    val fpgModel = new FPGrowthModel(dataset.sparkSession.createDataFrame(retVal, schema), itemSupport, count, id2Item)
      .setMinConfidence($(minConfidence))
    
    fpgModel
  }
  
  private def findFrequentItems(minCount: Long,
                        transactions: RDD[Array[Int]],
                        partitioner: Partitioner): Array[(Int, Long)] = {
    transactions.flatMap{ items =>
      items.flatMap(it => Iterator.single((it, 1L)))
    }.reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
  }
  
  private def findFrequentItemSets(minCount: Long,
                           transactions: RDD[Array[Int]],
                           frequentItems: Array[Int],
                           partitioner: Partitioner): RDD[FrequentSet] = {
    val item2Rank = frequentItems.zipWithIndex.toMap

    transactions.mapPartitionsWithIndex{ (partId, iter) =>
      var proCnt = 0L
      iter.flatMap{ items =>
        if (proCnt % 10000 == 0) {
          println(s"partition $partId $proCnt transactions processed")
        }
        proCnt += 1
        val ptrans = items.flatMap(item2Rank.get).sorted
        val partMap = mutable.HashMap.empty[Int, Array[Int]]
        val n = ptrans.length
        var i = n - 1
        while (i >= 0) {
          val item = ptrans(i)
          val partNum = partitioner.getPartition(item)
          if (!partMap.contains(partNum)) {
            partMap(partNum) = ptrans.slice(0, i + 1)
          }
          i -= 1
        }
        partMap
      }
    }.aggregateByKey(new FPTree, partitioner.numPartitions)(
      (tree, transaction) => tree.addToFPTree(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
      .flatMap{
        case (partNum, tree) =>
          tree.getPatterns(minCount, it => partitioner.getPartition(it) == partNum)
      }.map {case (ranks, count) =>
      new FrequentSet(ranks.map(i => frequentItems(i)).toArray, count)
    }
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object FPGrowth {
  private[fpm] class FrequentSet(val items: Array[Int],
                                 val freq: Long) extends Serializable {
    override def toString: String = {
      s"${items.mkString("{", ",", "}")}: $freq"
    }
  }
}
