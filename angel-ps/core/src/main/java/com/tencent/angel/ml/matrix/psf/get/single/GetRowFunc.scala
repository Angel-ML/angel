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

package com.tencent.angel.ml.matrix.psf.get.single

import java.util.List
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.impl.PSContext
import com.tencent.angel.psagent.matrix.ResponseType
import com.tencent.angel.psagent.matrix.transport.adapter.RowSplitCombineUtils
import scala.collection.JavaConversions._

/**
  * Get a matrix row function implements by the get udf.
  */
class GetRowFunc(param: GetParam) extends GetFunc(param) {
  def this() = this(null)

  def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val param = partParam.asInstanceOf[PartitionGetRowParam]
    val row = psContext.getMatrixStorageManager.getRow(param.getMatrixId, param.getRowIndex,
                                                              param.getPartKey.getPartitionId)
    new PartitionGetRowResult(row)
  }

  def merge(partResults: List[PartitionGetResult]): GetResult = {
    val rowSplits = partResults.map(_.asInstanceOf[PartitionGetRowResult].getRowSplit)

    val getRowParam: GetRowParam = getParam.asInstanceOf[GetRowParam]
    return new GetRowResult(ResponseType.SUCCESS,
                            RowSplitCombineUtils.combineServerRowSplits(rowSplits,
                                                                        getRowParam.getMatrixId,
                                                                        getRowParam.getRowIndex))
  }
}
