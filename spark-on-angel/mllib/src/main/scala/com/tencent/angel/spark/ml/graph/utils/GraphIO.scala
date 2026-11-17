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
package com.tencent.angel.spark.ml.graph.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object GraphIO {

  private val DELIMITER = "delimiter"
  private val HEADER = "header"

  private val int2Long = udf[Long, Int](_.toLong)
  private val string2Long = udf[Long, String](_.toLong)
  private val int2Float = udf[Float, Int](_.toFloat)
  private val long2Float = udf[Float, Long](_.toFloat)
  private val double2Float = udf[Float, Double](_.toFloat)
  private val string2Float = udf[Float, String](_.toFloat)
  private val long2Int = udf[Int, Long](_.toInt)
  private val string2Int = udf[Int, String](_.toInt)
  private val int2String = udf[String, Int](_.toString)
  private val long2String = udf[String, Long](_.toString)

  def convert2Float(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: LongType =>
        df.withColumn(tmpName, long2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: IntegerType =>
        df.withColumn(tmpName, int2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: DoubleType =>
        df.withColumn(tmpName, double2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: StringType =>
        df.withColumn(tmpName, string2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: FloatType => df
      case t => throw new Exception(s"$t can't convert to Float")
    }
  }

  def convert2Long(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: LongType => df
      case _: IntegerType =>
        df.withColumn(tmpName, int2Long(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: StringType =>
        df.withColumn(tmpName, string2Long(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case t => throw new Exception(s"$t can't convert to Long")
    }
  }

  def convert2Int(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: IntegerType => df
      case _: LongType =>
        df.withColumn(tmpName, long2Int(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: StringType =>
        df.withColumn(tmpName, string2Int(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case t => throw new Exception(s"$t can't convert to Long")
    }
  }

  def convert2String(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: StringType => df
      case _: LongType =>
        df.withColumn(tmpName, long2String(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: IntegerType =>
        df.withColumn(tmpName, int2String(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case t => throw new Exception(s"$t can't convert to String")
    }
  }

  def load(input: String, isWeighted: Boolean,
           srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
           sep: String = " ", hasEdgeType: Boolean = false, edgeTypeIndex: Int = 3): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    ; {
      val seq = new ArrayBuffer[StructField]()
      seq.append(StructField("src", LongType, nullable = false))
      seq.append(StructField("dst", LongType, nullable = false))
      if (hasEdgeType) seq.append(StructField("edgeType", LongType, nullable = false))
      if (isWeighted) seq.append(StructField("weight", FloatType, nullable = false))
      val schema = StructType(seq)
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  def loadWithTypeAndWeight(input: String, colNames: Array[String], level : Int, sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    ; {
      val schema = StructType(
        colNames.zipWithIndex.map{ case (name, idx) =>
          if (idx < 2)
            StructField(name, LongType, false)
          else if (name == "weight")
            StructField(name, FloatType, false)
          else StructField(name, IntegerType, false)
        }.toSeq)

      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  def loadString(input: String, index: Int = 0, minPartitions: Int = -1): RDD[String] = {
    val sc = SparkSession.builder().getOrCreate().sparkContext
    sc.textFile(input, if (minPartitions > 0) minPartitions else sc.defaultMinPartitions)
  }

  def loadStringWeight(input: String, isWeighted: Boolean,
                       srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
                       sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    ; {
      val schema = if (isWeighted) {
        StructType(Seq(
          StructField("src", LongType, nullable = false),
          StructField("dst", LongType, nullable = false),
          StructField("weight", StringType, nullable = false)
        ))
      } else {
        StructType(Seq(
          StructField("src", LongType, nullable = false),
          StructField("dst", LongType, nullable = false)
        ))
      }
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  def loadVertexType(input: String, srcIndex: Int = 0, dstIndex: Int = 1,
                     sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    ; {
      val schema =
        StructType(Seq(
          StructField("vertex", LongType, nullable = false),
          StructField("type", StringType, nullable = false)
        ))
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  def loadStringInfoLabel(input: String, isLabeled: Boolean,
                          srcIndex: Int = 0, dstIndex: Int = 1, infoIndex: Int = 2, labelIndex: Int = 3,
                          sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    ; {
      val schema = if (isLabeled) {
        StructType(Seq(
          StructField("src", LongType, nullable = false),
          StructField("dst", LongType, nullable = false),
          StructField("info", LongType, nullable = false),
          StructField("label", StringType, nullable = false)
        ))
      } else {
        StructType(Seq(
          StructField("src", LongType, nullable = false),
          StructField("dst", LongType, nullable = false),
          StructField("info", LongType, nullable = false)
        ))
      }
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  // user-item biGraph where the item ids are of string type
  def loadBiGraph(input: String, isWeighted: Boolean,
                  srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
                  sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    ; {
      val schema = if (isWeighted) {
        StructType(Seq(
          StructField("src", LongType, nullable = false),
          StructField("dst", StringType, nullable = false),
          StructField("weight", FloatType, nullable = false)
        ))
      } else {
        StructType(Seq(
          StructField("src", LongType, nullable = false),
          StructField("dst", StringType, nullable = false)
        ))
      }
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  def loadNode(input: String, index: Int = 0, sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    ; {
      val schema = StructType(Seq(StructField("node", LongType, nullable = false)))
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  def loadInt(input: String, isWeighted: Boolean,
              srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
              sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    ; {
      val schema = if (isWeighted) {
        StructType(Seq(
          StructField("src", IntegerType, nullable = false),
          StructField("dst", IntegerType, nullable = false),
          StructField("weight", FloatType, nullable = false)
        ))
      } else {
        StructType(Seq(
          StructField("src", IntegerType, nullable = false),
          StructField("dst", IntegerType, nullable = false)
        ))
      }
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  def loadNodeSeed(input: String, nodeIndex: Int = 0, sep: String = ""): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    ; {
      val se1 = Array(StructField("node", LongType, nullable = false))
      val schema = StructType(se1.toSeq)
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  def loadNodeAttrs(input: String, nodeIndex: Int = 0, attrIndexes: Array[Int], sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    ; {
      val se1 = Array(StructField("node", LongType, nullable = false))
      val se = attrIndexes.map(x => StructField("attr_" + x, FloatType, nullable = false))
      val schema = StructType((se1 ++ se).toSeq)
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  // load string type edge
  def loadStringEdge(input: String, isWeighted: Boolean,
                     srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
                     sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    ; {
      val schema = if (isWeighted) {
        StructType(Seq(
          StructField("src", StringType, nullable = false),
          StructField("dst", StringType, nullable = false),
          StructField("weight", FloatType, nullable = false)
        ))
      } else {
        StructType(Seq(
          StructField("src", StringType, nullable = false),
          StructField("dst", StringType, nullable = false)
        ))
      }
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }


  // load string2int map which was saved in the former string2int reindex process
  def loadString2IntMap(input: String, srcIndex: Int = 0, dstIndex: Int = 1, sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    ; {
      val schema = StructType(Seq(
        StructField("src", StringType, nullable = false),
        StructField("dst", IntegerType, nullable = false)
      ))
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }

  // load string2int map which was saved in the former string2int reindex process
  def loadString2LongMap(input: String, srcIndex: Int = 0, dstIndex: Int = 1, sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    ; {
      val schema = StructType(Seq(
        StructField("src", StringType, nullable = false),
        StructField("dst", LongType, nullable = false)
      ))
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    
}
  }





  def save(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    ; {
      df.write
        .mode(SaveMode.Overwrite)
        .option(HEADER, "false")
        .option(DELIMITER, seq)
        .csv(output)
    
}
  }

  def saveParquet(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    ; {
      df.write
        .mode(SaveMode.Overwrite)
        .option(HEADER, "false")
        .option(DELIMITER, seq)
        .parquet(output)
    
}
  }

  def appendSave(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    ; {
      df.write
        .mode(SaveMode.Append)
        .option(HEADER, "false")
        .option(DELIMITER, seq)
        .csv(output)
    
}
  }

  def appendSaveParquet(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    ; {
      df.write
        .mode(SaveMode.Append)
        .option(HEADER, "false")
        .option(DELIMITER, seq)
        .parquet(output)
    
}
  }

  def savePostMapping(path: String, partitionNum: Int, denseToString: RDD[(Int, String)]): Unit = {
    val conf = SparkContext.getOrCreate().getConf
    val keyValueSep = conf.get("spark.hadoop.angel.line.keyvalue.sep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "|"
    }
    val input = loadStringEdge(path, false, sep=keyValueSep)
    val inputRDD = input.select("src", "dst").rdd
      .map(f => (f.getString(0).toInt, f.getString(1)))
      .repartition(partitionNum)
      .persist()

    val output = inputRDD.join(denseToString)
      .map(r => Row.fromSeq(Seq[Any](r._2._2, r._2._1)))
      .persist()
    val outputNum = output.count()
    println(s"$outputNum records in output file.")
    val spark = SparkSession.builder.getOrCreate
    val schema = StructType(Seq(
      StructField("oid", StringType, nullable = false),
      StructField("embedding", StringType, nullable = false)
    ))
    val df = spark.createDataFrame(output, schema)
    save(df, path, seq=keyValueSep)
    inputRDD.unpersist()
    output.unpersist()
  }

  def savePostMappingByNodeType(path: String, partitionNum: Int, denseToString: RDD[(Int, String)], nodeTypeRDD: RDD[(Int, Int)]): Unit = {
    val conf = SparkContext.getOrCreate().getConf
    val keyValueSep = conf.get("spark.hadoop.angel.line.keyvalue.sep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "|"
    }
    val input = loadStringEdge(path, false, sep=keyValueSep)
    val inputRDD = input.select("src", "dst").rdd
      .map(f => (f.getString(0).toInt, f.getString(1)))
      .join(nodeTypeRDD)
      .join(denseToString)
      .repartition(partitionNum)
      .persist()
    val spark = SparkSession.builder.getOrCreate
    val schema = StructType(Seq(
      StructField("oid", StringType, nullable = false),
      StructField("embedding", StringType, nullable = false)
    ))
    val all_nodeTypes = nodeTypeRDD.map(_._2).distinct().collect()
    all_nodeTypes.foreach{node_type =>
      val sub_output = inputRDD.filter(f => f._2._1._2 == node_type)
        .map(r => Row.fromSeq(Seq[Any](r._2._2, r._2._1._1)))
      val df = spark.createDataFrame(sub_output, schema)
      save(df, path + "_node_" + node_type.toString, seq=keyValueSep)
    }
    inputRDD.unpersist()
  }

  def saveByNodeType(path: String, partitionNum: Int, nodeTypeRDD: RDD[(Int, Int)]): Unit = {
    val conf = SparkContext.getOrCreate().getConf
    val keyValueSep = conf.get("spark.hadoop.angel.line.keyvalue.sep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "|"
    }
    val input = loadStringEdge(path, false, sep=keyValueSep)
    val inputRDD = input.select("src", "dst").rdd
      .map(f => (f.getString(0).toInt, f.getString(1)))
      .join(nodeTypeRDD)
      .repartition(partitionNum)
      .persist()
    val spark = SparkSession.builder.getOrCreate
    val schema = StructType(Seq(
      StructField("nid", StringType, nullable = false),
      StructField("embedding", StringType, nullable = false)
    ))
    val all_nodeTypes = nodeTypeRDD.map(_._2).distinct().collect()
    all_nodeTypes.foreach{node_type =>
      val sub_output = inputRDD.filter(f => f._2._2 == node_type)
        .map(r => Row.fromSeq(Seq[Any](r._1.toString, r._2._1)))
      val df = spark.createDataFrame(sub_output, schema)
      save(df, path + "_node_" + node_type.toString, seq=keyValueSep)
    }
    inputRDD.unpersist()
  }

  def defaultCheckpointDir: Option[String] = {
    val sparkContext = SparkContext.getOrCreate()
    sparkContext.getConf.getOption("spark.yarn.stagingDir")
      .map { base =>
        new Path(base, s".sparkStaging/${sparkContext.getConf.getAppId}").toString
      }
  }



  def loadEdgesWithStringNodeId(dataset: Dataset[_],
                                srcNodeIdCol: String,
                                dstNodeIdCol: String,
                                needReplicateEdges: Boolean = false
                               ): RDD[(String, String)] = {
    val edges =
      dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getString(0), row.getString(1)))
        .filter(f => f._1 != f._2)

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
    }
    else {
      edges
    }
  }

  def loadEdgesWithIntNodeId(dataset: Dataset[_],
                             srcNodeIdCol: String,
                             dstNodeIdCol: String,
                             needReplicateEdges: Boolean = false
                            ): RDD[(Int, Int)] = {
    val edges =
      dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getString(0).toInt, row.getString(1).toInt))
        .filter(f => f._1 != f._2)

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
    }
    else {
      edges
    }
  }

  def loadEdgesWithWeightStringNodeId(dataset: Dataset[_],
                                      srcNodeIdCol: String,
                                      dstNodeIdCol: String,
                                      weightCol: String,
                                      hasWeight: Boolean = true,
                                      needReplicateEdges: Boolean = false
                                     ): RDD[(String, String, Float)] = {
    val edges =
      if (hasWeight) {
        dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getString(0), row.getString(1), row.getFloat(2)))
          .filter(f => f._1 != f._2 && f._3 != 0.0f)
      } else {
        dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getString(0), row.getString(1), 1.0f))
          .filter(f => f._1 != f._2)
      }

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2, f._3), (f._2, f._1, f._3)))
    }
    else {
      edges
    }
  }

  def loadEdgesWithWeight(dataset: Dataset[_],
                          srcNodeIdCol: String,
                          dstNodeIdCol: String,
                          weightCol: String,
                          hasWeight: Boolean = true,
                          needReplicateEdges: Boolean = false
                         ): RDD[(Long, Long, Float)] = {
    val edges =
      if (hasWeight) {
        dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
          .filter(f => f._1 != f._2 && f._3 != 0.0f)
      } else {
        dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getLong(0), row.getLong(1), 1.0f))
          .filter(f => f._1 != f._2)
      }

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2, f._3), (f._2, f._1, f._3)))
    }
    else {
      edges
    }
  }

  def loadEdges(dataset: Dataset[_],
                srcNodeIdCol: String,
                dstNodeIdCol: String,
                needReplicateEdges: Boolean = false) : RDD[(Long, Long)] = {
    val edges = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)

    if (needReplicateEdges) {
      edges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
    }
    else {
      edges
    }

  }

  def loadEdgesFromDF(dataset: Dataset[_],
                      srcNodeIdCol: String,
                      dstNodeIdCol: String,
                      needReplicateEdges: Boolean = false) : RDD[(Long, Long)] = {
    loadEdges(dataset, srcNodeIdCol, dstNodeIdCol, needReplicateEdges)
  }

  def loadEdgesWithWeightFromDF(dataset: Dataset[_],
                                srcNodeIdCol: String,
                                dstNodeIdCol: String,
                                weightCol: String,
                                hasWeight: Boolean = true,
                                needReplicateEdges: Boolean = false): RDD[(Long, Long, Float)] = {
    loadEdgesWithWeight(dataset, srcNodeIdCol, dstNodeIdCol, weightCol, hasWeight, needReplicateEdges)
  }
}