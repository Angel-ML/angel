package com.tencent.angel.spark.ml.featureEngineering.Scaler

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2020-11-16
 */
object ScalerLocal {

  //minMaxScaler
  val MINMAX = "minmax"
  //standardScaler
  val STANDARD = "standard"
  //standard HDFS path
  val STANDARDPATH = "standardPath"

  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "data/mlDataTest/data1_test_scaler.txt"
    val sampleRate = 1
    val partitionNum = 1
    val output = "data/output/output"
    val sep = ","

    //feature conf path for minmaxScaler and standardScaler
    val scaleConfPath = "data/mlDataTest/scaleConf.txt"

    //path for mean and variance of features which is computed by this model
    //if this path exists,we will use it,if not,we will save the values on it
    //val standardPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/standardPath"
    val standardPath = null

    if (scaleConfPath != null) {

      val sc = start(mode)
      run(sc, input, output, partitionNum, sampleRate, scaleConfPath, standardPath, sep)
      stop()
    }


    def run(sc: SparkContext,
            input: String,
            output: String,
            partitionNum: Int,
            sampleRate: Double,
            scaleConfPath: String,
            standardPath: String,
            sep: String) {

      val dataFrame = DataLoader.loadTable(sc, input, partitionNum, sampleRate, null, sep).rdd
        .map { case row: Row => row.toSeq.toArray.map(x => x.toString) }.cache()

      //Array[(Int, String, Double, Double, Boolean, Boolean)]:id,type,min,max,isStd,isMean,MeanValue,standardValue
      var summary: MultivariateStatisticalSummary = null
      var fConf: Array[(Int, String, Double, Double, Boolean, Boolean, String, String)] = Array()
      if (scaleConfPath != null) {
        fConf = ScalerJsonParser.parserJson(scaleConfPath)

        val fConfId = fConf.map(x => x._1)
        //feature value must be Double
        val fConf2DF = dataFrame.map { x =>
          val valf = fConfId.map(i => x(i).toDouble)
          Vectors.dense(valf)
        }

        //statistic all the conf feature
        summary = Statistics.colStats(fConf2DF)
      }

      //three condition:(1)no standard scaler in conf file and no standard path,just conduct minmax
      //(2)no standard scaler in conf file but there is standard path,read the path and complete feature conf
      //(3)there is standard scaler in conf file,just read the conf file and save standard conf in the path
      //if it exists
      val standConfLeng = fConf.filter(conf => conf._2 == STANDARD).length
      if (standConfLeng != 0 && standardPath != null) {
        val standConfStart = fConf.length - standConfLeng

        val summaryStandarded = (0 until standConfLeng).map { i =>
          //decide the stardard feature position
          val standIndex = standConfStart + i
          Array(fConf(standIndex)._1.toString,
            fConf(standIndex)._5.toString,
            fConf(standIndex)._6.toString,
            summary.mean(standIndex).toString,
            summary.variance(standIndex).toString)
        }

        //save to the path
        val standardRdd = sc.makeRDD(summaryStandarded, 1)
        DataSaver.save(standardRdd, standardPath, sep)
      }
      else if (standConfLeng == 0 && standardPath != null) {
        //read the path
        val pathConf = DataLoader.loadTable(sc, standardPath, 1, 1.0, null, sep).rdd
          .map { row: Row => row.toSeq.toArray.map(x => x.toString) }
          .collect

        //complete the feature conf
        val fConfBuffer = fConf.toBuffer
        pathConf.map { x =>
          fConfBuffer += Tuple8(x(0).toInt, STANDARD, 0.0, 0.0, x(1).toBoolean, x(2).toBoolean, x(3), x(4))
        }

        //update fConf
        fConf = fConfBuffer.toArray
      }

      val resultDF = dataFrame.map { f =>
        f2Scaler(f, summary, fConf, standardPath)
      }
      DataSaver.save(resultDF, output, sep)

      dataFrame.unpersist()

    }

    //according to feature conf to transform the original data
    def f2Scaler(fArray: Array[String],
                 summary: MultivariateStatisticalSummary,
                 fConf: Array[(Int, String, Double, Double, Boolean, Boolean, String, String)],
                 standardPath: String): Array[String] = {
      fArray.indices.map { i =>
        var afterScalerValue = fArray(i)

        fConf.indices.foreach { j =>
          // if actual id equal to one conf id,then change the feature value
          if (i == fConf(j)._1) {
            afterScalerValue = fConf(j)._2 match {
              case MINMAX =>
                minMaxScaler(fArray(i).toDouble, summary.min(j), summary.max(j), fConf(j)._3, fConf(j)._4).toString
              case STANDARD =>
                // standard for whether to mean and variance and meanValue and varianceValue
                val standardValue = if (fConf(j)._7 != null && fConf(j)._8 != null)
                  (fConf(j)._7.toDouble, fConf(j)._8.toDouble)
                else
                  (summary.mean(j), summary.variance(j))

                standardScaler(fArray(i).toDouble, Math.sqrt(standardValue._2), standardValue._1, fConf(j)._5, fConf(j)._6).toString
            }
          }
        }
        afterScalerValue
      }.toArray

    }

    def minMaxScaler(fVal: Double, actMin: Double, actMax: Double, userMin: Double, userMax: Double): Double = {
      val originalRange = actMax - actMin
      val scale = userMax - userMin
      //if actual min equals to actual max,all the value is half of the new defined range
      val raw = if (originalRange != 0) (fVal - actMin) / originalRange else 0.5
      raw * scale + userMin
    }

    //actStd:the sample standard deviation
    def standardScaler(fVal: Double, actStd: Double, actMean: Double, userStd: Boolean, userMean: Boolean): Double = {
      var values = fVal
      if (userMean) {
        if (userStd) {
          values = if (actStd != 0.0) (fVal - actMean) * (1.0 / actStd) else 0.0
        }
        else
          values -= actMean
      } else if (userStd) {
        val raw = if (actStd != 0.0) 1.0 / actStd else 0.0
        values *= raw
      }
      values
    }

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
