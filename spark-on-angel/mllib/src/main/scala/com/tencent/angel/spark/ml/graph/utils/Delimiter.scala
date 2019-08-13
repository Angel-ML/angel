package com.tencent.angel.spark.ml.graph.utils

import org.apache.spark.{SparkConf, SparkContext}

object Delimiter {

  val COMMA = "comma"
  val COMMA_VAL = ","
  val SPACE = "space"
  val SPACE_VAL = " "
  val SEMICOLON = "semicolon"
  val SEMICOLON_VAL = ";"
  val TAB = "tab"
  val TAB_VAL = "\t"
  val PIPE = "pipe"
  val PIPE_VAL = "|"
  val DOUBLE_QUOTE = "double_quote"
  val DOUBLE_QUOTE_VAL = "\""
  val STAR = "star"
  val STAR_VAL = "*"
  val map = Map(
    COMMA -> COMMA_VAL,
    SPACE -> SPACE_VAL,
    SEMICOLON -> SEMICOLON_VAL,
    TAB -> TAB_VAL,
    PIPE -> PIPE_VAL,
    DOUBLE_QUOTE -> DOUBLE_QUOTE_VAL,
    STAR -> STAR_VAL
  )

  def parse(sep: String): String = {
    val delimiter = map.getOrElse(sep, SPACE_VAL)
    println(s"""set delimiter to $sep -> "$delimiter"""")
    delimiter
  }
}
