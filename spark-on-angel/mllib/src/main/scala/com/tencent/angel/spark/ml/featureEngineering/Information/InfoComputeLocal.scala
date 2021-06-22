package com.tencent.angel.spark.ml.featureEngineering.Information

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by isakjiang on 2020-11-16
 * colStr can not be null
 */
object InfoComputeLocal {

  def main(args: Array[String]) {

    val mode = "local"
    val input = "data/mlDataTest/wine.data"
    val output = "data/output/output"
    val sampleRate = 1
    val partitionNum = 1
    val featureCols = "1-10"

    //目标标签所在列，仅一列,从0开始计数,正例值为1，负例值为0,between beginCol and endCol
    val labelCol = 0
    val sep = ","

    val sc = start(mode)
    run(sc, input, output, partitionNum, sampleRate, featureCols, labelCol, sep)
    stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          partitionNum: Int,
          sampleRate: Double,
          colStr: String,
          objectCol: Int,
          sep: String) {
    //the input cols
    val feaColArray = DataLoader.parseFeatureCols(colStr)
      .flatMap { case (begin, end) => begin to end }
    val inputColArray = Array(objectCol) ++ feaColArray

    val catLabelFea = objectCol.toString + "," + colStr
    val inputDF = DataLoader.loadTable(sc, input, partitionNum, sampleRate, catLabelFea, sep)

    //每行vector => Array[string]
    val inputRDD = inputDF.rdd.map { case row: Row => row.toSeq.toArray.map(x => x.toString) }

    //经过提取后实际的目标所在列
    val actualCol = 0

    //对特征值封装成FeatureValue,id为所在列的列号
    val formatedFeaVal = FeatureFormat.formatFeatureValue(inputRDD, actualCol)

    //同一个特征id封装成一个feature类
    val formatedFea = FeatureFormat.formatFeature(formatedFeaVal)

    //获得目标列的信息分布,输出：(正例数量、负例数量、正负总样例数)
    val objDis = objDistribute(formatedFea.filter(f => f.id == actualCol).first())

    //除目标列外的指标计算,每行结果为（featureId, IGR, GI, MI, SU）
    val resultInfo = formatedFea.filter(f => f.id != actualCol).map { oneFeature =>
      //计算信息增益率
      val IGR = new InfoGainRatio(oneFeature, objDis)
      val resultIGR = IGR.Compute
      //计算基尼系数
      val GI = new GiniIndex(oneFeature, objDis)
      val resultGI = GI.Compute
      //计算互信息
      val MI = new MutualInformation(oneFeature, objDis)
      val resultMI = MI.Compute
      //计算SymmetryUncertainty
      val SU = new SymmetryUncertainty(oneFeature, objDis)
      val resultSU = SU.Compute

      Array(inputColArray(oneFeature.id).toString,
        resultIGR.toString,
        resultGI.toString,
        resultMI.toString,
        resultSU.toString)
    }

    // save the result at driver and add label information on the first row
    val resultArray = ArrayBuffer[Array[String]]()
    val labelArray = Array("X", "IGR", "GI", "MI", "SU")
    resultArray += labelArray

    val inforCol = resultInfo.collect()
    resultArray ++= inforCol

    val resultRdd = sc.makeRDD(resultArray, 1)
    DataSaver.save(resultRdd, output, sep)
  }

  //统计目标列的正负样例数
  def objDistribute(obj: Feature): (Int, Int, Int) = {
    var labelPos = 0
    var labelNeg = 0
    var labelPosNeg = 0

    for (iter <- obj.values) {
      labelPos += iter.pos
      labelNeg += iter.neg
      labelPosNeg += iter.posneg
    }
    (labelPos, labelNeg, labelPosNeg)
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