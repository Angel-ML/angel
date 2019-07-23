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

import com.tencent.angel.spark.automl.feature.cross.FeatureCrossOp
import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.DataType

class SelfCartesian(override val uid: String)
  extends UnaryTransformer[Vector, Vector, SelfCartesian] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("SelfCartesian"))

  override protected def createTransformFunc: Vector => Vector = FeatureCrossOp.flatCartesian

  override protected def outputDataType: DataType = new VectorUDT()
}

object SelfCartesian extends DefaultParamsReadable[SelfCartesian] {

  @Since("1.6.0")
  override def load(path: String): SelfCartesian = super.load(path)
}
