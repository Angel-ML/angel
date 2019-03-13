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


package com.tencent.angel.spark.automl.feature

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DataLoader(ss: SparkSession) {
  def load(input: String, separator: String): DataFrame

  def load(input: String): DataFrame = load(input, " ")
}

case class LibSVMDataLoader(ss: SparkSession) extends DataLoader(ss) {
  override def load(input: String, separator: String): DataFrame = {
    ss.read.format("libsvm").load(input)
  }
}

case class CSVDataLoader(ss: SparkSession) extends DataLoader(ss) {
  override def load(input: String, separator: String): DataFrame = {
    ss.read.csv(input)
  }
}

case class JSONDataLoader(ss: SparkSession) extends DataLoader(ss) {
  override def load(input: String, separator: String): DataFrame = {
    ss.read.json(input)
  }
}

case class DocumentDataLoader(ss: SparkSession) extends DataLoader(ss) {
  override def load(input: String, separator: String): DataFrame = {
    ss.createDataFrame(
      ss.sparkContext.textFile(input).map(Tuple1.apply)
    ).toDF("sentence")
  }
}

case class LabeledDocumentDataLoader(ss: SparkSession) extends DataLoader(ss) {
  override def load(input: String, separator: String): DataFrame = {
    require(separator.equals(","),
      "the label and sentence should be separated by comma")
    ss.createDataFrame(
      ss.sparkContext.textFile(input)
        .map { line =>
          val splits = line.split(separator)
          (splits(0), splits(1))
        })
      .toDF("label", "sentence")
  }

  override def load(input: String): DataFrame = load(input, ",")
}

case class SimpleDataLoader(ss: SparkSession) extends DataLoader(ss) {
  override def load(input: String, separator: String): DataFrame = {
    ss.createDataFrame(
      ss.sparkContext.textFile(input)
        .map(_.split(separator)).map(Tuple1.apply)
    ).toDF("features")
  }
}

case class LabeledSimpleDataLoader(ss: SparkSession) extends DataLoader(ss) {
  override def load(input: String, separator: String): DataFrame = {
    ss.createDataFrame(
      ss.sparkContext.textFile(input)
        .map { line =>
          val splits = line.split(separator)
          (splits.head, splits.tail)
        }
    ).toDF("label", "features")
  }
}


object DataLoader {

  def load(ss: SparkSession,
           format: String,
           input: String,
           separator: String = " "): DataFrame = {
    format match {
      case "libsvm" => LibSVMDataLoader(ss).load(input)
      case "csv" => CSVDataLoader(ss).load(input)
      case "json" => JSONDataLoader(ss).load(input)
      case "document" => DocumentDataLoader(ss).load(input, separator)
      case "label-document" => LabeledDocumentDataLoader(ss).load(input, separator)
      case "simple" => SimpleDataLoader(ss).load(input, separator)
      case "label-simple" => LabeledSimpleDataLoader(ss).load(input, separator)
      case _ => SimpleDataLoader(ss).load(input, separator)
    }
  }

}
