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
package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasTruncLength extends Params {
  /**
   * Param for truncation length.
   *
   * @group param
   */
  final val truncLength = new IntParam(this, "truncLength", "truncLength")

  /** @group getParam */
  final def getTruncLength: Int = $(truncLength)

  setDefault(truncLength, 6000)

  /** @group setParam */
  final def setTruncLength(size: Int): this.type = set(truncLength, size)
}
