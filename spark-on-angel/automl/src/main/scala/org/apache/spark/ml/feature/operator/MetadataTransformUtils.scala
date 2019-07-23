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

import org.apache.spark.sql.types.{MetadataBuilder, StructField}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ArrayBuffer

/**
  * The factory to record the generation information for each feature in the pipeline
  */
object MetadataTransformUtils {

  final val DERIVATION: String = "derivation"

  /**
    * Check and initialization.
    * @param numFeatures: the number of features
    * @return
    */
  private def createDerivation(numFeatures: Int): Array[String] = {
    val arrayBuffer = ArrayBuffer[String]()
    (0 until numFeatures).foreach{ i =>
      arrayBuffer.append("f_" + i.toString)
    }
    arrayBuffer.toArray
  }

  /**
    *
    * @param feature1
    * @param feature2
    * @return Array[String]
    */
  private def cartesianWithArray(feature1: Array[String], feature2: Array[String]): Array[String] = {
    val res = ArrayBuffer[String]()
    feature1.foreach{ f1 =>
      feature2.foreach{ f2 =>
        res.append("(" + f1 + " x " + f2 + ")")
      }
    }
    res.toArray
  }

  /**
    *
    * @param field
    * @param filterIndices
    * @param numFeatures
    * @return
    */
  def featureSelectionTransform(field: StructField, // Metadata is private[types]
                                filterIndices: Array[Int],
                                numFeatures: Int): MetadataBuilder = {
    val metadata = field.metadata

    var derivation = Array[String]()
    if (metadata.contains(DERIVATION)) {
      derivation = filterIndices map metadata.getStringArray(DERIVATION)
    } else {
      derivation = createDerivation(numFeatures)
    }

    new MetadataBuilder().withMetadata(metadata).putStringArray(DERIVATION, derivation)
  }

  /**
    *
    * @param fields: The Array[StructField]
    * @param numFeatures number of features
    * @return
    */
  def vectorCartesianTransform(fields: Array[StructField], numFeatures: Int): MetadataBuilder = {
    if (fields.length < 2) {
      throw new IllegalArgumentException("the number of cols in the input DataFrame should be no less than 2")
    }

    var res = Array[String]()
    if (fields.head.metadata.contains(DERIVATION)) {
      res = fields.head.metadata.getStringArray(DERIVATION)
    } else {
      res = createDerivation(numFeatures)
    }

    for (i <- 1 until fields.length) {
      if (fields(i).metadata.contains(DERIVATION)) {
        res = cartesianWithArray(res, fields(i).metadata.getStringArray(DERIVATION))
      } else {
        res = cartesianWithArray(res, createDerivation(numFeatures))
      }
    }

    val metadata = fields.last.metadata
    new MetadataBuilder().withMetadata(metadata).putStringArray(DERIVATION, res)
  }

}
