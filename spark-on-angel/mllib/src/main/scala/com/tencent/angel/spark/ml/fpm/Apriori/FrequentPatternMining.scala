package com.tencent.angel.spark.ml.fpm.Apriori


import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Created by endy on 2019/1/15.
  */
object FrequentPatternMining {
  def findOneLengthFrequentSets(originalData: RDD[String],
                                delimiter: String,
                                support: Double,
                                sort: Int): RDD[(String, Double)] = {
    val transcationNum = originalData.count()
    val freqThreshold: Int = Math.round(transcationNum * support).toInt
    val itemCount = originalData
      .map(x => x.split(delimiter).toSet)
      .flatMap(_.toTraversable)
      .map(item => (item, 1))
      .reduceByKey(_ + _)
      .filter(pattern => pattern._2 >= freqThreshold)
      .map(x => (x._1, x._2 * 1.0 / transcationNum))
    val itemSort = sort match {
      case 1 => itemCount.sortBy(_._2, ascending = true)
      case -1 => itemCount.sortBy(_._2, ascending = false)
      case 0 => itemCount
    }
    itemSort
  }

  def findTwoLengthItemsetsRDD(
                                data: RDD[Array[String]],
                                delimiter: String,
                                freqThreshold: Int,
                                sort: Int): RDD[(String, Int)] = {
    val largeSet = data.mapPartitions(partition => {
      // [A,B,C] => [A|B,A|C,B|C]
      partition.flatMap(transaction => {
        val candidates = new ArrayBuffer[(String, Int)]
        for (i <- transaction.indices) {
          for (j <- i + 1 until transaction.length) {
            candidates += ((transaction(i) + delimiter + transaction(j), 1))
          }
        }
        candidates.iterator
      })
    }).reduceByKey(_ + _)
      .filter(x => x._2 >= freqThreshold)
    //        .sortByKey()
    val itemSort = sort match {
      case 1 => largeSet.sortBy(_._2, ascending = true)
      case -1 => largeSet.sortBy(_._2, ascending = false)
      case 0 => largeSet
    }
    itemSort
  }

  def apriori(originalData: RDD[String],
              delimiter: String,
              support: Double,
              maxLength: Int,
              sort: Int): RDD[(String, Double)] = {
    val freqItems = findOneLengthFrequentSets(originalData, delimiter, support, sort)
    if (maxLength == 1) {
      return freqItems
    }
    val sc = originalData.sparkContext
    val transcationNum = originalData.count()
    val freqThreshold: Int = Math.round(transcationNum * support).toInt
    val data = originalData.map(x => x.split(delimiter).distinct.sorted)
    var largeSet = findTwoLengthItemsetsRDD(data, delimiter, freqThreshold, sort)
    var freqItemsets = largeSet
    if (freqItemsets.count() == 0) {
      return freqItemsets.map(x => (x._1, x._2 * 1.0 / transcationNum))
    }
    var isComplete = false
    var k = 3

    while (k <= maxLength && !isComplete) {
      val candidates = selfJoin(largeSet, delimiter).collect()
      if (candidates.isEmpty) {
        isComplete = true
      } else {
        // start a new round
        val bcCandidates = sc.broadcast(candidates.sorted)
        val kFrequentSetRDD = data.mapPartitions(transactions => {
          val candidatesArray = bcCandidates.value
          val itemset = new mutable.HashSet[String]()
          candidatesArray.foreach(x => {
            val items = x.split(delimiter)
            itemset.++=(items)
          })

          val candidateTrie = new CandidateTrie

          candidates.sorted.foreach(x => candidateTrie.insert(x.split(delimiter)))
          transactions.flatMap(transaction => {
            candidateTrie.search(transaction.filter(x => itemset.contains(x)), delimiter)
              .filter(x => !x.isEmpty).map(x => (x, 1))
          })
        }).reduceByKey(_ + _)
          .filter(x => x._2 >= freqThreshold)
        //            .sortByKey()

        // add kfrequent itemsets to all-frequent itemset
        if (kFrequentSetRDD.count() == 0) {
          isComplete = true
        } else {
          val sortKFrequentSetRDD = sort match {
            case 1 => kFrequentSetRDD.sortBy(_._2, ascending = true)
            case -1 => kFrequentSetRDD.sortBy(_._2, ascending = false)
            case 0 => kFrequentSetRDD
          }
          freqItemsets = freqItemsets.union(sortKFrequentSetRDD)
          largeSet = sortKFrequentSetRDD
          k += 1
        }
      }
    } // end while

    freqItems ++ freqItemsets.map(x => (x._1, x._2 * 1.0 / transcationNum))
  }


  def binarySearch(map: Array[(String, Int)], s: String): Int = {
    var lo = 0
    var hi = map.length - 1

    while (lo <= hi) {
      val mid = (lo + hi) / 2

      val value = s.compareTo(map(mid)._1)

      if (value == 0) {
        return mid
      } else if (value < 0) {
        hi = mid - 1
      } else if (value > 0) {
        lo = mid + 1
      }
    }
    -1
  }

  // given frequent itemsets to generate candidates by self-joining
  def selfJoin(largeSets: RDD[(String, Int)], delimiter: String): RDD[String] = {
    // transfer to pair RDD
    val removeFirst = largeSets.map(x => {
      // remove the first item
      val pattern = x._1
      var i = 0
      var isFound = false
      while (i < pattern.length && !isFound) {
        if (pattern(i) == delimiter.charAt(0)) {
          isFound = true
        } else {
          i = i + 1
        }
      }

      val key = pattern.substring(i + 1)
      (key, pattern)

    })

    val removeLast = largeSets.map(x => {
      // remove last item
      val pattern = x._1
      var i = pattern.length - 1
      var isFound = false

      while (i >= 0 && !isFound) {
        if (pattern(i) == delimiter.charAt(0)) {
          isFound = true
        } else {
          i = i - 1
        }
      }
      val key = pattern.substring(0, i)
      (key, pattern)
    })

    val jointSet = removeFirst.join(removeLast).map(x => {
      val s1 = x._2._1
      val s2 = x._2._2

      // get the last item of s2
      val items = s2.split(delimiter)
      val lastItem = items(items.length - 1)

      // return by appending lastItem to s1
      s1 + delimiter + lastItem
    })
    jointSet
  }

  // an itemset is represented by <items,...,items>
  def itemsetToString(set: Array[String], delimiter: String): String = {
    val sb = new StringBuilder

    if (set.isEmpty) {
      return ""
    }
    if (set.length == 1) {
      return "" + set.toSeq.head
    }
    set.foreach(x => {
      sb.append(x)
      sb.append(delimiter)
    })
    sb.deleteCharAt(sb.length - 1)
    sb.toString()
  }
}
