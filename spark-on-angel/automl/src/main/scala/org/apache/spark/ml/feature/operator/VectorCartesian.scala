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
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import scala.collection.mutable.ArrayBuilder

class VectorCartesian (override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("VectorCartesian"))

  /** @group setParam */
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  // optimistic schema; does not contain any ML attributes
  override def transformSchema(schema: StructType): StructType = {
    require(get(inputCols).isDefined, "Input cols must be defined first.")
    require(get(outputCol).isDefined, "Output col must be defined first.")
    require($(inputCols).length > 0, "Input cols must have non-zero length.")
    require($(inputCols).map(c => schema(c).dataType).forall(_.sameType(VectorType)),
      "Input cols must be vectors.")
    StructType(schema.fields :+ StructField($(outputCol), new VectorUDT, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val startTime = System.currentTimeMillis()
    transformSchema(dataset.schema, logging = true)
    val inputFeatures = $(inputCols).map(c => dataset.schema(c))
    val vectorDims = getVectorDimension(dataset, ${inputCols})
    val featureAttrs = getFeatureAttrs(inputFeatures)

    def interactFunc = udf { row: Row =>
      var indices = ArrayBuilder.make[Int]
      var values = ArrayBuilder.make[Double]
      var size = 1
      indices += 0
      values += 1.0
      var featureIndex = row.length - 1
      while (featureIndex >= 0) {
        val prevIndices = indices.result()
        val prevValues = values.result()
        val prevSize = size
        indices = ArrayBuilder.make[Int]
        values = ArrayBuilder.make[Double]
        size *= vectorDims(featureIndex)
        processNonzeroOutput(row(featureIndex), (i, a) => {
          var j = 0
          while (j < prevIndices.length) {
            indices += prevIndices(j) + i * prevSize
            values += prevValues(j) * a
            j += 1
          }
        })
        featureIndex -= 1
      }
      Vectors.sparse(size, indices.result(), values.result()).compressed
    }

    val featureCols = inputFeatures.map { f => dataset(f.name) }

    dataset.select(
      col("*"),
      interactFunc(struct(featureCols: _*)).as($(outputCol), featureAttrs.toMetadata()))
  }

  private def getVectorDimension(dataset: Dataset[_], cols: Array[String]): Array[Int] = {
    dataset.head match {
      case row: Row =>
        cols.map { col =>
          row.getAs[Vector](col).size
//          match {
//            case dv: DenseVector => dv.size
//            case sv: SparseVector => sv.size
//          }
        }
      case _ => throw new SparkException(s"item in dataset should be Row.")
    }
  }

  /**
    * Given an input row of features, invokes the specific function for every non-zero output.
    *
    * @param value The row value to encode, either a Double or Vector.
    * @param f The callback to invoke on each non-zero (index, value) output pair.
    */
  def processNonzeroOutput(value: Any, f: (Int, Double) => Unit): Unit = value match {
    case vec: Vector =>
      vec.foreachActive { (i, v) =>
        f(i, v)
      }
    case null =>
      throw new SparkException("Vectors cannot be null.")
    case o =>
      throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
  }

  /**
    * Generates ML attributes for the output vector of all feature interactions. We make a best
    * effort to generate reasonable names for output features, based on the concatenation of the
    * interacting feature names and values delimited with `_`. When no feature name is specified,
    * we fall back to using the feature index (e.g. `foo:bar_2_0` may indicate an interaction
    * between the numeric `foo` feature and a nominal third feature from column `bar`.
    *
    * @param features The input feature columns to the Interaction transformer.
    */
  private def getFeatureAttrs(features: Seq[StructField]): AttributeGroup = {
    var featureAttrs: Seq[Attribute] = Nil
    features.reverse.foreach { f =>
      val encodedAttrs = f.dataType match {
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(f)
          encodedFeatureAttrs(
            group.attributes.getOrElse(new Array[Attribute](0)), Some(group.name))
        case _ =>
          throw new SparkException(s"Col ${f.name} must be Vector.")
      }
      if (featureAttrs.isEmpty) {
        featureAttrs = encodedAttrs
      } else {
        featureAttrs = encodedAttrs.flatMap { head =>
          featureAttrs.map { tail =>
            NumericAttribute.defaultAttr.withName(head.name.get + ":" + tail.name.get)
          }
        }
      }
    }
    new AttributeGroup($(outputCol), featureAttrs.toArray)
  }

  /**
    * Generates the output ML attributes for a single input feature. Each output feature name has
    * up to three parts: the group name, feature name, and category name (for nominal features),
    * each separated by an underscore.
    *
    * @param inputAttrs The attributes of the input feature.
    * @param groupName Optional name of the input feature group (for Vector type features).
    */
  private def encodedFeatureAttrs(
                                   inputAttrs: Array[Attribute],
                                   groupName: Option[String]): Seq[Attribute] = {

    def format(
                index: Int,
                attrName: Option[String],
                categoryName: Option[String]): String = {
      val parts = Seq(groupName, Some(attrName.getOrElse(index.toString)), categoryName)
      parts.flatten.mkString("_")
    }

    inputAttrs.zipWithIndex.flatMap {
      case (a: Attribute, i) =>
        Seq(NumericAttribute.defaultAttr.withName(format(i, a.name, None)))
    }
  }

  override def copy(extra: ParamMap): VectorCartesian = defaultCopy(extra)

}

@Since("1.6.0")
object VectorCartesian extends DefaultParamsReadable[VectorCartesian] {

  @Since("1.6.0")
  override def load(path: String): VectorCartesian = super.load(path)
}
