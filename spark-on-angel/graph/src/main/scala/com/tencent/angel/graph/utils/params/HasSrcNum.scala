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

trait HasSrcNum extends Params {

  /**
    * Param for partitionNum.
    *
    * @group param
    */
  final val srcNum = new IntParam(this, "srcNum",
    "num of sample sources (k) for Closeness algorithm")

  /** @group getParam */
  final def getSrcNum: Int = $(srcNum)

  setDefault(srcNum, 20)

  /** @group setParam */
  final def setSrcNum(num: Int): this.type = set(srcNum, num)

}
