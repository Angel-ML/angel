/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.matrix.psf.get.multi

import java.util
import java.util.{HashMap, List, Map}

import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.impl.PSContext
import com.tencent.angel.ps.impl.matrix.ServerRow
import com.tencent.angel.psagent.matrix.ResponseType
import com.tencent.angel.psagent.matrix.transport.adapter.RowSplitCombineUtils
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.JavaConversions._

/**
  * Get a batch of rows function implements by the get udf.
  */
object GetRowsFunc {
  private val LOG: Log = LogFactory.getLog(classOf[GetRowsParam])
}

class GetRowsFunc(param: GetParam) extends GetFunc(param) {

  def this() = this(null)
  def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val rowsParam = partParam.asInstanceOf[PartitionGetRowsParam]
    val rowIndexes: List[Integer] = rowsParam.getRowIndexes

    val rows:List[ServerRow] = rowIndexes.map(PSContext.get.getMatrixPartitionManager.getRow(rowsParam.getMatrixId, _, rowsParam.getPartKey.getPartitionId))
    new PartitionGetRowsResult(rows)
  }

  def merge(partResults: List[PartitionGetResult]): GetResult = {
    val rowIndexToSplitsMap: Map[Int, List[ServerRow]] = new HashMap[Int, List[ServerRow]]
    val size = partResults.size
    for (i <- 0 until size) {
      val rowSplits: List[ServerRow] = (partResults.get(i).asInstanceOf[PartitionGetRowsResult]).getRowSplits
      rowSplits.map(row => {
        if(rowIndexToSplitsMap.containsKey(row.getRowId)) {
          rowIndexToSplitsMap.get(row.getRowId).add(row)
        } else {
          var rowList = new util.ArrayList[ServerRow]()
          rowList.add(row)
          rowIndexToSplitsMap += row.getRowId -> rowList
        }
      })
    }

    def mergedRows: Map[Integer, TVector] = rowIndexToSplitsMap map{
        case(k,v) => (k.asInstanceOf[Integer] -> RowSplitCombineUtils.combineServerRowSplits(v, param.matrixId, k))
    }


    return new GetRowsResult(ResponseType.SUCCESS, mergedRows)
  }
}
