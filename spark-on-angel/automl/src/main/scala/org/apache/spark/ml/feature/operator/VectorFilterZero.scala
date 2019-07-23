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

package org.apache.spark.ml.feature.operator

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute, UnresolvedAttribute}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, ArrayBuilder}

class VectorFilterZero(var featureMap: Map[Int, Int], override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this(featureMap: Map[Int, Int]) = this(featureMap, Identifiable.randomUID("VectorFilterZero"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    // Schema transformation.
    val schema = dataset.schema
    val attrs = VectorFilterZero.getAttrs(dataset, ${inputCol})
    val metadata = new AttributeGroup($(outputCol), attrs).toMetadata()

    val nnzIndices = VectorFilterZero.getNonZero(dataset, $(inputCol))

    featureMap ++= nnzIndices.zipWithIndex.toMap
    println(s"feature map:")
    println(featureMap.mkString(","))

    // Data transformation.
    val filterFunc = udf { r: Row =>
      val vec = r.get(0).asInstanceOf[Vector]
      VectorFilterZero.filter(featureMap, vec)
    }
    val args = Array($(inputCol)).map { c =>
      schema(c).dataType match {
        case _: VectorUDT => dataset(c)
      }
    }

    dataset.select(col("*"), filterFunc(struct(args: _*)).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    val inputDataType = schema(inputColName).dataType
    if (!inputDataType.isInstanceOf[VectorUDT]) {
      throw new IllegalArgumentException(s"Data type $inputDataType is not supported.")
    }
    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, new VectorUDT, true))
  }

  override def copy(extra: ParamMap): VectorAssembler = defaultCopy(extra)
}

object VectorFilterZero extends DefaultParamsReadable[VectorFilterZero]{

  override def load(path: String): VectorFilterZero = super.load(path)

  private def getAttrs(dataset: Dataset[_], inputCol: String): Array[Attribute] = {
    val schema = dataset.schema
    lazy val first = dataset.toDF.first()
    val field = schema(inputCol)
    val index = schema.fieldIndex(inputCol)
    field.dataType match {
      case _: VectorUDT =>
        val group = AttributeGroup.fromStructField(field)
        if (group.attributes.isDefined) {
          // If attributes are defined, copy them with updated names.
          group.attributes.get.zipWithIndex.map { case (attr, i) =>
            if (attr.name.isDefined) {
              // TODO: Define a rigorous naming scheme.
              attr.withName(inputCol + "_" + attr.name.get)
            } else {
              attr.withName(inputCol + "_" + i)
            }
          }
        } else {
          // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
          // from metadata, check the first row.
          val numAttrs = group.numAttributes.getOrElse(first.getAs[Vector](index).size)
          Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName(inputCol + "_" + i))
        }
      case otherType =>
        throw new SparkException(s"VectorFilterZero does not support the $otherType type")
    }
  }

  private def getNonZero(dataset: Dataset[_],
                         column: String): Array[Int] = {
    dataset.select(column).rdd.mapPartitions { rows: Iterator[Row] =>
      val mergeIndices = rows.map{ case Row(v: Vector) =>
        v match {
          case sv: SparseVector =>
            sv.indices
          case _ => throw new IllegalArgumentException(s"Input column $column should be SparseVector.")
        }
      }.reduce(_ union _ distinct)
      Iterator(mergeIndices)
    }.collect().reduce((a, b) => (a union b).distinct).sortBy(x => x)
  }

  private def filter(featureMap: Map[Int, Int], vec: Vector): Vector = {
    val indices = ArrayBuilder.make[Int]
    val values = ArrayBuilder.make[Double]
    vec match {
      case vec: Vector =>
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += featureMap(i)
            values += v
          }
        }
      case null =>
        // TODO: output Double.NaN?
        throw new SparkException("Vector to filter cannot be null.")
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
    }
    Vectors.sparse(featureMap.size, indices.result(), values.result()).compressed
  }

}

