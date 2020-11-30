/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.graph.utils


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
