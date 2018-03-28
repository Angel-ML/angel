/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving.common

import com.tencent.angel.ml.matrix.RowType

/**
  * the model meta
  * @param matricesMeta the matrix meta of model
  */
case class ModelMeta(matricesMeta: Array[MatrixMeta]) {

}

/**
  * the matrix meta
  * @param name the matrix name
  * @param rowType the matrix type
  * @param rowNum the row num
  * @param dimension the dimension
  */
case class MatrixMeta(name: String, rowType: RowType, rowNum: Int, dimension: Long, acceptInput:Boolean=true)
