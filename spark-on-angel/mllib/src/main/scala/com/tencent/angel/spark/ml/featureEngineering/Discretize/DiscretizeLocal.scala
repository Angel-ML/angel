package com.tencent.angel.spark.ml.featureEngineering.Discretize

import java.util

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2020-11-16
 */
object DiscretizeLocal {

  //等频离散
  val DISCRETE_EQU_FRE = "equFre"
  //等值离散
  val DISCRETE_EQU_VAL = "equVal"
  //训练集离散化边界保存路径
  val DISBOUNDSPATH = "disBoundsPath"

  def main(args: Array[String]) {
    val mode = "local"
    val input = "data/mlDataTest/wine.data"
    val output = "data/output/output"
    val partitionNum = 1
    val sampleRate = 1
    val sep = ","

    //特征配置文件路径
    val featureConfName = "data/mlDataTest/DiscreteJson.txt"
    val disBoundsPath = "data/output/output_discre_disBoundsPath"

    val sc = start(mode)
    run(sc, input, output, partitionNum, sampleRate, featureConfName, disBoundsPath, sep)
    stop()


  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          partitionNum: Int,
          sampleRate: Double,
          featureConfName: String,
          disBoundsPath: String,
          sep: String): Unit = {

    //解析特征配置文件,特征id从0开始,(featureId,discreteType,numBin,min,max)
    val fConfFilterNon = DiscreteJsonParser.parser(featureConfName)
    //等频离散配置
    val fConfFre = fConfFilterNon.filter(x => x._2 == DISCRETE_EQU_FRE)
    //等值离散配置
    val fConfVal = fConfFilterNon.filter(x => x._2 == DISCRETE_EQU_VAL)

    val inputDF = DataLoader
      .loadTable(sc, input, partitionNum, sampleRate, null, sep).rdd
      .map { case row: Row => row.toSeq.toArray.map(x => x.toString) }.cache()

    val valBounds: Array[(Int, Array[Double])] = if (fConfVal.length != 0) {
      //equal value discrete dataframe
      val equConfName = fConfVal.map(x => x._1)
      //feature value must be Double
      val equValDF = inputDF.map { x =>
        val valf = equConfName.map(i => x(i).toDouble)
        Vectors.dense(valf)
      }

      //min and max value for all features
      val summary = Statistics.colStats(equValDF)

      //equal_value bounds
      (0 until fConfVal.length).map { i =>
        val valMin = summary.min(i)
        val valMax = summary.max(i)
        //user defined min and max
        val userMin = fConfVal(i)._4
        val userMax = fConfVal(i)._5
        val fId = fConfVal(i)._1
        if (userMin != null && valMin < userMin.toDouble)
          throw new Exception(s"Wrong Min Feature Value:$valMin at id:$fId")
        else if (userMax != null && valMax > userMax.toDouble)
          throw new Exception(s"Wrong Max Feature Value:$valMax at id:$fId")
        (fId, equValDiscrete(fConfVal(i)._3, valMin, valMax))
      }.toArray
    } else
      Array()

    //最终的离散边界,等频配置存在才计算样本个数
    val actBounds = if (fConfFre.length != 0) {
      val fRowsCount = inputDF.count()
      //traverse
      val freBounds = fConfFre.map { fC =>
        //提取fC中的Id
        val fId = fC._1
        val tempRdd = inputDF.map { arrStr => arrStr(fId).toDouble }.sortBy(x => x).zipWithIndex
        val numBin = fC._3
        val tempBounds = equFreDiscrete(tempRdd, numBin, fRowsCount)
        //如果等频离散桶数不合理，即离散边界有重复
        if (tempBounds.distinct.length < (numBin - 1))
          throw new Exception(s"Wrong Bin Number of Equal Frequence Discrete:$numBin at id:$fId")
        else
          (fId, tempBounds)
      }
      freBounds ++ valBounds
    } else
      valBounds

    //if the output is null,then avoid save rdd
    if (output != null) {
      val result = inputDF.map { case arrStr =>
        discretizeInput(arrStr, actBounds)
      }
      //保存离散化后数据
      DataSaver.save(result, output, sep)
    }
    inputDF.unpersist()

    //每行为:特征Id+"："+对应的离散边界(以空格相隔),仅保存在hdfs
    if (disBoundsPath != null) {
      val resultBounds = actBounds.map(x => Array(x._1.toString, x._2.mkString(" ")))
      val actBoundRdd = sc.makeRDD(resultBounds, 1)
      DataSaver.save(actBoundRdd, disBoundsPath, sep)
    }
  }

  //等频离散
  def equFreDiscrete(fIdValue: RDD[(Double, Long)], numBin: Int, fRowsCount: Long): Array[Double] = {
    //每个桶的数量
    val width = fRowsCount / numBin
    val numBounds = numBin - 1
    val bounds = fIdValue.filter(x => (x._2 + 1) % width == 0).map(_._1).collect
    //提取边界
    (0 until numBounds).map(i => bounds(i)).toArray
  }

  //等值离散
  def equValDiscrete(numBin: Int, min: Double, max: Double): Array[Double] = {
    //每个桶的数量
    val width = (max - min) / numBin
    val numBounds = numBin - 1
    (0 until numBounds).map(x => min + (x + 1) * width).toArray
  }

  //对原始数据进行离散化
  def discretizeInput(fArray: Array[String], actBounds: Array[(Int, Array[Double])]): Array[String] = {
    (0 until fArray.length).map { i =>
      var afterDisValue = fArray(i)
      //if the feature exist in the actBounds
      for (oneBound <- actBounds) {
        if (i == oneBound._1) {
          afterDisValue = binSearch(oneBound._2, afterDisValue).toString
        }
      }
      afterDisValue
    }.toArray
  }

  //binarySearch
  def binSearch(boundarys: Array[Double], x: String): Int = {
    val sIndex = util.Arrays.binarySearch(boundarys, x.toDouble)
    if (sIndex >= 0) sIndex else -sIndex - 1
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