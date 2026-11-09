package com.tencent.angel.spark.ml.fpm.Apriori


import org.apache.spark.rdd.RDD
import com.tencent.angel.spark.ml.fpm.Apriori.FrequentPatternMining._


object MineAssociateRuleBatch {
  def associatedRules(
                       data: RDD[String],
                       delimiter: String,
                       support: Double,
                       confidence: Double,
                       maxLength: Int,
                       sort: Int): RDD[(String, Double, Double, Double)] = {
    val sc = data.sparkContext
    val transcationNum = data.count()
    val freqThreshold = Math.round(transcationNum * support).toInt
    val data1 = data.map(x => x.split(delimiter))
    val freqItems = FrequentPatternMining.findOneLengthFrequentSets(data, delimiter, support, sort)
    val freqItemsets = apriori(data, delimiter, support, maxLength, sort)
    val allFreqItemsets = freqItems.union(freqItemsets).sortByKey()
      .map(x => (x._1, Math.round(x._2 * transcationNum).toInt)).collect()
    assert(confidence <= 1 && confidence > 0)
    val allFreqItemsetsBC = sc.broadcast(allFreqItemsets)
    val rules = freqItemsets.mapPartitions(partition => {
      val allFreqItemsetsValue = allFreqItemsetsBC.value
      partition.flatMap(x =>
        findAssociatedRulesFromItemset(transcationNum, x._1,
          allFreqItemsetsValue, delimiter, confidence))
    })
    rules.map(x => (x._1, x._2, x._3, x._4))
  }

  def findAssociatedRulesFromItemset(totalNum: Long,
                                     pattern: String,
                                     freqItemsets: Array[(String, Int)],
                                     delimiter: String,
                                     confidence: Double):
  Iterator[(String, Double, Double, Double)] = {
    val items = pattern.split(delimiter).toSet
    val patternCount = binarySearchForCounts(pattern, freqItemsets)
    val subsets = items.subsets(items.size - 1)
    val result = subsets.map(subset => {
      val X = FrequentPatternMining.itemsetToString(subset.toArray.sorted, delimiter)
      val Y = items.diff(subset).head
      val X_Count = binarySearchForCounts(X, freqItemsets)
      val Y_Count = binarySearchForCounts(Y, freqItemsets)
      val confidenceValue = patternCount / X_Count
      val lift = confidenceValue / (Y_Count / totalNum)
      (X + "=>" + Y, patternCount / totalNum, confidenceValue, lift)
    }).filter(x => x._3 >= confidence)
    result
  }

  def binarySearchForCounts(target: String, patterns: Array[(String, Int)]): Double = {
    if (patterns == null || patterns.isEmpty) {
      return -1
    }
    var lo = 0
    var hi = patterns.length
    while (lo <= hi) {
      val mid = (lo + hi) / 2
      if (patterns(mid)._1 == target) {
        return patterns(mid)._2.toDouble
      } else if (target < patterns(mid)._1) {
        hi = mid - 1
      } else {
        lo = mid + 1
      }
    }
    -1
  }
}
