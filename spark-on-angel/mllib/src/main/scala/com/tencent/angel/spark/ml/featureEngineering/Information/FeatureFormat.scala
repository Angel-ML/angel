package com.tencent.angel.spark.ml.featureEngineering.Information

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by allylu on 2016/10/27.
 * 此类的用途：
 * 统计feature中的内容，一个特征值封装成一个FeatureValue类，最终封装成Feature类
 */
object FeatureFormat {

  //正例值
  val posValue = 1
  //负例值
  val negValue = 0

  //输入数据封装成FeatureValue
  def formatFeatureValue(inputRDD: RDD[Array[String]], objectCol: Int): RDD[FeatureValue] = {
    val result = inputRDD.map(line => parseFeatureValue(line, objectCol))
      .flatMap(line => line)
      .map(fv => idValueKey(fv))
      .reduceByKey(posNegReduce) //统计同一类别的正负样例数
      .map(pair => pair._2)
    result
  }

  //每个特征值封装到FeatureValue中，组成数组
  def parseFeatureValue(line: Array[String], objectCol: Int): List[FeatureValue] = {
    var list = new mutable.ListBuffer[FeatureValue]
    // 该行特征所对应的正负例
    val objectValue = line(objectCol).toDouble.toInt

    //统计包括目标列在内的特征值
    for (i <- 0 until line.size) {
      if (!line(i).isEmpty) {
        var featureValue = new FeatureValue
        featureValue.id = i
        featureValue.value = line(i)
        if (objectValue.equals(posValue)) {
          featureValue.pos = 1
          featureValue.neg = 0
        } else if (objectValue.equals(negValue)) {
          featureValue.pos = 0
          featureValue.neg = 1
        }
        featureValue.posneg = 1
        list += featureValue
      }
    }
    list.toList
  }

  def idValueKey(fv: FeatureValue): ((Int, String), FeatureValue) = {
    new Tuple2((fv.id, fv.value), fv)
  }

  //同一特征下同一类别的正负样例数量合并
  def posNegReduce(fv1: FeatureValue, fv2: FeatureValue): FeatureValue = {
    val result = new FeatureValue
    result.id = fv1.id
    result.value = fv1.value

    result.pos += fv1.pos + fv2.pos
    result.neg += fv1.neg + fv2.neg
    result.posneg += fv1.posneg + fv2.posneg
    result
  }

  //FeatureValue封装成Feature类
  def formatFeature(fv: RDD[FeatureValue]): RDD[Feature] = {
    //以featureValue.Id进行汇总
    val featureArray = fv.map(data => featureMap(data))
      .reduceByKey(featureReduce)
      .map(pair => pair._2)
    featureArray
  }

  def featureMap(fv: FeatureValue): (Int, Feature) = {
    val result = new Feature
    result.id = fv.id
    result.values += fv
    (result.id, result)
  }

  def featureReduce(f1: Feature, f2: Feature): Feature = {
    val result = new Feature
    result.id = f1.id
    result.values ++= f1.values
    result.values ++= f2.values
    result
  }

}
