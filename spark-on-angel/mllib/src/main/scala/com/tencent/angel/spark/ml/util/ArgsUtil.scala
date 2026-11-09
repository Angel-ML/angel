package com.tencent.angel.spark.ml.util

object ArgsUtil {

  /**
   * the string “1-10,12,15” is for 1 to 10,12 and 15 columns,
   * will return Array((1, 10), (12, 12), (15, 15))
   * @param colExpr
   * @return feature sections, Array[(begin, end)]
   */
  def parseFeatureCols(colExpr: String): Array[(Int, Int)] = {
    if (colExpr == null || colExpr == "") {
      null
    } else {
      colExpr.trim.split(SEP_COMMA, -1)
        .filter(_ != "")
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
          Tuple2(begin, end)
        }
    }
  }


}
