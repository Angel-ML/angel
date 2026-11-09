package com.tencent.angel.spark.ml.featurePreprocess.FillMissingValue

import scala.io.Source
import scala.util.parsing.json.JSON
import com.google.gson.JsonParser
import com.tencent.angel.spark.ml.util.{SEP_COMMA, SEP_HYPHEN}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by isakjiang on 2020-11-16
 */
object FillMissingValueConf {

  //the output content is feature ids, missingValue, fillMethod
  // here fillMethod include: mean、median、count
  def parser(featureConfName: String): Array[(String, String, String)] = {

    println("the actual conf name is: " + featureConfName)
    //读配置文件
    val jsonString = Source.fromFile(featureConfName, "utf-8").mkString
    //解析成json字符串格式
    val json = new JsonParser()
    val jsonObject = json.parse(jsonString).getAsJsonObject

    val featureJson = jsonObject.getAsJsonArray("feature")
    val resultConf = new ArrayBuffer[(String, String, String)]

    //sparse for featureJson
    if (featureJson != null) {
      val featureIter = featureJson.iterator()
      while (featureIter.hasNext) {
        val intemTmp = featureIter.next().getAsJsonObject
        val id = intemTmp.get("id").getAsString
        val fillMethod = if (intemTmp.get("fillMethod") != null) intemTmp.get("fillMethod").getAsString else null
        val missingValue = if (intemTmp.get("missingValue") != null) intemTmp.get("missingValue").getAsString else null
        /*
        for (i <- parseFeatureCols(id)) {
          resultConf += Tuple3(i.toString, missingValue, fillMethod)
        }
         */
        resultConf += Tuple3(id, missingValue, fillMethod)
      }
    }

    resultConf.toArray
  }


  /**
   * the string “1-10,12,15” is for 1 to 10,12 and 15 columns,
   * will return Array(1,2,3,4,5,6,7,8,9,10,12,15)
   *
   * @param colExpr
   * @return feature sections, Array[feature col id]
   */
  def parseFeatureCols(colExpr: String): Array[Int] = {


    val colIDs = if (colExpr == null || colExpr == "") {
      null
    } else {
      colExpr.trim.split(SEP_COMMA, -1).filter(_ != "")
        .map { section =>
          val items = section.trim.split(SEP_HYPHEN, -1).map(_.toInt)
          var begin = -1
          var end = -1
          if (items.length == 1) {
            begin = items(0)
            end = items(0)
          } else if (items.length == 2) {
            begin = items(0)
            end = items(1)
            require(begin < end, s"feature col format is wrong(begin must less than end): $section")
          } else {
            new Exception(s"feature col format is wrong: $colExpr")
          }
          Array.range(begin, end + 1)
        }
    }
    colIDs.flatten
  }
}
