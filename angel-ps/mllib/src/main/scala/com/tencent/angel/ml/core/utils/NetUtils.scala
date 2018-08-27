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


package com.tencent.angel.ml.core.utils

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.matrix.RowType


object NetUtils {
  def keyType(modelType: RowType): String = {
    modelType match {
      case RowType.T_DOUBLE_SPARSE | RowType.T_DOUBLE_DENSE | RowType.T_DOUBLE_DENSE_COMPONENT | RowType.T_DOUBLE_SPARSE_COMPONENT => "int"
      case RowType.T_DOUBLE_DENSE_LONGKEY_COMPONENT | RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT => "long"
      case RowType.T_FLOAT_SPARSE | RowType.T_FLOAT_DENSE | RowType.T_FLOAT_DENSE_COMPONENT | RowType.T_FLOAT_SPARSE_COMPONENT => "int"
      case RowType.T_FLOAT_DENSE_LONGKEY_COMPONENT | RowType.T_FLOAT_SPARSE_LONGKEY | RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT => "long"
      case RowType.T_LONG_SPARSE | RowType.T_LONG_DENSE | RowType.T_LONG_DENSE_COMPONENT | RowType.T_LONG_SPARSE_COMPONENT => "int"
      case RowType.T_LONG_DENSE_LONGKEY_COMPONENT | RowType.T_LONG_SPARSE_LONGKEY | RowType.T_LONG_SPARSE_LONGKEY_COMPONENT => "long"
      case RowType.T_INT_SPARSE | RowType.T_INT_DENSE | RowType.T_INT_DENSE_COMPONENT | RowType.T_INT_SPARSE_COMPONENT => "int"
      case RowType.T_INT_DENSE_LONGKEY_COMPONENT | RowType.T_INT_SPARSE_LONGKEY | RowType.T_INT_SPARSE_LONGKEY_COMPONENT => "long"
      case _ => throw new AngelException("Key Type is not supported!")
    }
  }

  def valueType(modelType: RowType): String = {
    modelType match {
      case RowType.T_DOUBLE_SPARSE | RowType.T_DOUBLE_DENSE | RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_DOUBLE_DENSE_COMPONENT |
           RowType.T_DOUBLE_DENSE_LONGKEY_COMPONENT | RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT | RowType.T_DOUBLE_SPARSE_COMPONENT |
           RowType.T_DOUBLE_SPARSE_COMPONENT => "double"
      case RowType.T_FLOAT_SPARSE | RowType.T_FLOAT_DENSE | RowType.T_FLOAT_SPARSE_LONGKEY | RowType.T_FLOAT_DENSE_COMPONENT |
           RowType.T_FLOAT_DENSE_LONGKEY_COMPONENT | RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT | RowType.T_FLOAT_SPARSE_COMPONENT |
           RowType.T_FLOAT_SPARSE_COMPONENT => "float"
      case RowType.T_LONG_SPARSE | RowType.T_LONG_DENSE | RowType.T_LONG_SPARSE_LONGKEY | RowType.T_LONG_DENSE_COMPONENT |
           RowType.T_LONG_DENSE_LONGKEY_COMPONENT | RowType.T_LONG_SPARSE_LONGKEY_COMPONENT | RowType.T_LONG_SPARSE_COMPONENT |
           RowType.T_LONG_SPARSE_COMPONENT => "long"
      case RowType.T_INT_SPARSE | RowType.T_INT_DENSE | RowType.T_INT_SPARSE_LONGKEY | RowType.T_INT_DENSE_COMPONENT |
           RowType.T_INT_DENSE_LONGKEY_COMPONENT | RowType.T_INT_SPARSE_LONGKEY_COMPONENT | RowType.T_INT_SPARSE_COMPONENT |
           RowType.T_INT_SPARSE_COMPONENT => "int"
      case _ => throw new AngelException("Key Type is not supported!")
    }
  }

  def storageType(modelType: RowType): String = {
    modelType match {
      case RowType.T_DOUBLE_SPARSE | RowType.T_FLOAT_SPARSE | RowType.T_LONG_SPARSE | RowType.T_INT_SPARSE |
           RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_FLOAT_SPARSE_LONGKEY | RowType.T_LONG_SPARSE_LONGKEY |
           RowType.T_INT_SPARSE_LONGKEY => "sparse"
      case RowType.T_DOUBLE_DENSE | RowType.T_FLOAT_DENSE | RowType.T_LONG_DENSE | RowType.T_INT_DENSE => "dense"
      case RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT | RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT |
           RowType.T_LONG_SPARSE_LONGKEY_COMPONENT | RowType.T_INT_SPARSE_LONGKEY_COMPONENT |
           RowType.T_DOUBLE_SPARSE_COMPONENT | RowType.T_FLOAT_SPARSE_COMPONENT => "component_sparse"
      case RowType.T_DOUBLE_DENSE_COMPONENT | RowType.T_FLOAT_DENSE_COMPONENT | RowType.T_LONG_DENSE_COMPONENT |
           RowType.T_INT_DENSE_COMPONENT | RowType.T_DOUBLE_DENSE_LONGKEY_COMPONENT | RowType.T_FLOAT_DENSE_LONGKEY_COMPONENT |
           RowType.T_LONG_DENSE_LONGKEY_COMPONENT | RowType.T_INT_DENSE_LONGKEY_COMPONENT => "component_dense"
    }
  }

  def getDenseModelType(modelType: RowType): RowType = {
    NetUtils.valueType(modelType) match {
      case "double" => RowType.T_DOUBLE_DENSE
      case "float" => RowType.T_FLOAT_DENSE
      case "long" => RowType.T_LONG_DENSE
      case "int" => RowType.T_INT_DENSE
    }
  }

  def newDenseVector(modelType: RowType, dim: Int): Vector = {
    valueType(modelType) match {
      case "double" =>
        VFactory.denseDoubleVector(dim)
      case "float" =>
        VFactory.denseFloatVector(dim)
      case "long" =>
        VFactory.denseLongVector(dim)
      case "int" =>
        VFactory.denseIntVector(dim)
    }
  }

  def newDenseMatrix(modelType: RowType, numRows: Int, numCols: Int): Matrix = {
    valueType(modelType) match {
      case "double" =>
        MFactory.denseDoubleMatrix(numRows, numCols)
      case "float" =>
        MFactory.denseFloatMatrix(numRows, numCols)
    }
  }
}
