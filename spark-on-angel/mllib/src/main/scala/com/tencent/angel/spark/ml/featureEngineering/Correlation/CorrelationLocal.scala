package com.tencent.angel.spark.ml.featureEngineering.Correlation

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by isakjiang on 2020-11-16
 */
object CorrelationLocal {

  //method for correlation
  val METHOD = "method"
  val PEARSON = "pearson"
  val SPEARMAN = "spearman"

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/mlDataTest/wine.data"
    val output = "data/output/output"
    val partitionNum = 1
    val sampleRate = 1
    val sep = ","

    val newColStr = "1-5"
    val oriColStr = "7-9"
    val method = SPEARMAN

    val sc = start(mode)
    run(sc, input, output, partitionNum, sampleRate, newColStr, oriColStr, method, sep)
    stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          partitionNum: Int,
          sampleRate: Double,
          newColStr: String,
          oriColStr: String,
          method: String,
          sep: String) = {

    //new cols
    val parseCols = DataLoader.parseFeatureCols(newColStr)
    val newCols = if (parseCols != null) parseCols.flatMap { case (begin, end) => begin to end } else null

    //if new cols exist,compute the corr with each other
    if (newCols != null) {

      val newColLeng = newCols.length
      val eachArray = ArrayBuffer[Array[String]]()
      val betArray = ArrayBuffer[Array[String]]()
      //the last result to save on hdfs
      val resultArray = ArrayBuffer[Array[String]]()

      //if the number of new column is more than one,then compute the each corr
      if (newColLeng > 1) {
        val input2Vector = DataLoader.loadTable(sc, input, partitionNum, sampleRate, newColStr, sep).rdd
          .map { row: Row => Vectors.dense(row.toSeq.toArray.map(x => x.toString.toDouble)) }

        //cache the input rdd before compute corr when the method = "spearman"
        if (method == SPEARMAN) input2Vector.cache()
        val corrResult = Statistics.corr(input2Vector, method)
        if (method == SPEARMAN) input2Vector.unpersist()

        val matNum = corrResult.numRows
        // add lineId in case of confuse row column
        val singVecArray = (0 until matNum).map { i =>
          val rowArray = (0 until matNum).map { j => corrResult(i, j).toString }.toArray
          val iArray = Array(newCols(i).toString)
          iArray ++ rowArray
        }
        //        val resultRdd = sc.makeRDD(singVecArray)
        //        DataSaver.save(resultRdd, outputEach)

        //complete the first row in eachCorr
        val firstEachCorr = Array("X") ++ newCols.map(_.toString)
        eachArray += firstEachCorr
        eachArray ++= singVecArray

      }

      //original cols
      val parseCols = DataLoader.parseFeatureCols(oriColStr)
      // if original cols exist,compute the corr between new cols and original cols
      if (parseCols != null) {
        val oriCols = parseCols.flatMap { case (begin, end) => begin to end }
        val allCols = newCols ++ oriCols

        val input4All = DataLoader.loadTable(sc, input, partitionNum, sampleRate, newColStr + "," + oriColStr, sep).rdd
          .map { row: Row => row.toSeq.toArray.map(x => x.toString.toDouble) }.cache()

        val betCorr = if (method == PEARSON) {
          //statistic
          val stat = Statistics.colStats(input4All.map(x => Vectors.dense(x)))
          corr4Pearson(input4All, newCols, allCols.length, stat)
        } else {
          //get the rank for spearman
          val input4Vec = input4All.map(x => Vectors.dense(x))
          val rank4All = computeCorrelationMatrix(input4Vec)
          val stat = Statistics.colStats(rank4All)
          val rank4Array = rank4All.map(_.toArray)
          corr4Pearson(rank4Array, newCols, allCols.length, stat)
        }

        input4All.unpersist()
        val firstBetCorr = Array("X") ++ oriCols.map(_.toString)
        betArray += firstBetCorr
        betArray ++= betCorr

      }

      //concat eachArray and betArray,which will be saved on resultArray
      if ((eachArray.length != 0) && (betArray.length != 0)) {
        //delete the first column in betArray
        val betDelFirst = betArray.map { iArray =>
          (1 until iArray.length).map(i => iArray(i))
        }

        (0 until eachArray.length).map { i =>
          resultArray += (eachArray(i) ++ betDelFirst(i))
        }
      }
      else if (eachArray.length != 0)
        resultArray ++= eachArray
      else if (betArray.length != 0)
        resultArray ++= betArray

      //save the result if resultArray is not null
      if (resultArray.length != 0) {
        val resultRdd = sc.makeRDD(resultArray, 1)
        DataSaver.save(resultRdd, output, sep)
      }
    }
  }

  //each row represents the whole one to original table
  def corr4Pearson(input4All: RDD[Array[Double]],
                   newCols: Array[Int],
                   allColsLeng: Int,
                   stat: MultivariateStatisticalSummary): IndexedSeq[Array[String]] = {
    val sampleCount = stat.count.toDouble
    val newColsLeng = newCols.length

    val allOne2Ori = input4All.map { x =>
      val arrayCov = new ArrayBuffer[Double]

      (0 until newColsLeng).map { i =>
        (newColsLeng until allColsLeng).map { j =>
          val newVar = Math.sqrt(stat.variance(i))
          val oriVar = Math.sqrt(stat.variance(j))
          val cov = (x(i) - stat.mean(i)) * (x(j) - stat.mean(j))

          arrayCov += (cov / (newVar * oriVar))
        }
      }
      arrayCov.toArray
    }
    //accumulate
    val all2OriCorr = allOne2Ori
      .treeReduce { case (x1, x2) =>
        (0 until x1.length).map { i => x1(i) + x2(i) }.toArray
      }
      .map(x => (x * (1.0 / (sampleCount - 1))).toString)

    //regulate the result [(1,6),(1,7),(2,6),(2,7)]=>[(1,(6,7));(2,(6,7))]
    val oriLeng = allColsLeng - newColsLeng
    (0 until newColsLeng).map { i =>
      val one2ArrayCorr = (0 until oriLeng).map(x => all2OriCorr(i * oriLeng + x)).toArray
      Array(newCols(i).toString) ++ one2ArrayCorr
    }
  }

  def computeCorrelationMatrix(X: RDD[Vector]): RDD[Vector] = {
    // ((columnIndex, value), rowUid)
    val colBased = X.zipWithUniqueId().flatMap { case (vec, uid) =>
      vec.toArray.zipWithIndex.map { case (v, j) =>
        ((j, v), uid)
      }
    }
    // global sort by (columnIndex, value)
    val sorted = colBased.sortByKey()
    // assign global ranks (using average ranks for tied values)
    val globalRanks = sorted.zipWithIndex().mapPartitions { iter =>
      var preCol = -1
      var preVal = Double.NaN
      var startRank = -1.0
      var cachedUids = ArrayBuffer.empty[Long]
      val flush: () => Iterable[(Long, (Int, Double))] = () => {
        val averageRank = startRank + (cachedUids.size - 1) / 2.0
        val output = cachedUids.map { uid =>
          (uid, (preCol, averageRank))
        }
        cachedUids.clear()
        output
      }
      iter.flatMap { case (((j, v), uid), rank) =>
        // If we see a new value or cachedUids is too big, we flush ids with their average rank.
        if (j != preCol || v != preVal || cachedUids.size >= 10000000) {
          val output = flush()
          preCol = j
          preVal = v
          startRank = rank
          cachedUids += uid
          output
        } else {
          cachedUids += uid
          Iterator.empty
        }
      } ++ flush()
    }
    // Replace values in the input matrix by their ranks compared with values in the same column.
    // Note that shifting all ranks in a column by a constant value doesn't affect result.
    val groupedRanks = globalRanks.groupByKey().map { case (uid, iter) =>
      // sort by column index and then convert values to a vector
      Vectors.dense(iter.toSeq.sortBy(_._1).map(_._2).toArray)
    }
    groupedRanks
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }


}
