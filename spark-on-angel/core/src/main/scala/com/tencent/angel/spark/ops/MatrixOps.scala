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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.spark.ops

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.aggr.{FullPull, PullWithRows}
import com.tencent.angel.ml.matrix.psf.aggr.enhance.{FullAggrResult, SBAggrResult}
import com.tencent.angel.ml.matrix.psf.common.{Fill, Increment}
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.{Diag, Eye, FullFill, Random}
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.matrix.PSMatrix

class MatrixOps {

  /**
   * Initialize a random matrix, whose value is a random(0.0, 1.0)
   */
  def random(mat: PSMatrix): Unit = {
    mat.assertValid()
    update(mat, new Random(mat.id))
  }


  /**
   * Pull matrix to local
   */
  def pull(mat: PSMatrix): Array[Array[Double]] = {
    mat.assertValid()
    aggregate(mat, new FullPull(mat.id)).asInstanceOf[FullAggrResult].getResult
  }

  /**
   * pull multi rows to local
   */
  def pull(mat: PSMatrix, rows: Array[Int]): Array[(Int, Array[Double])] = {
    mat.assertValid()
    val sbResult = aggregate(mat, new PullWithRows(mat.id, rows)).asInstanceOf[SBAggrResult]
    val resData = sbResult.getData
    val resRows = sbResult.getRowIds
    resRows.zip(resData)
  }


  /**
   * Push local data to update matrix in PS
   */
  def push(mat: PSMatrix, pairs: Array[(Int, Long, Double)]): Unit = {
    mat.assertValid()

    val rows = new Array[Int](pairs.length)
    val cols = new Array[Long](pairs.length)
    val values = new Array[Double](pairs.length)
    pairs.indices.foreach { i =>
      rows(i) = pairs(i)._1
      cols(i) = pairs(i)._2
      values(i) = pairs(i)._3
    }
    update(mat, new Fill(mat.id, rows, cols, values))
  }

  def pull(mat: PSMatrix, pairs: Array[(Int, Long)]): Unit = {

  }

  def increment(mat: PSMatrix, pairs: Array[(Int, Long, Double)]): Unit = {
    mat.assertValid()

    val rows = new Array[Int](pairs.length)
    val cols = new Array[Long](pairs.length)
    val values = new Array[Double](pairs.length)
    pairs.indices.foreach { i =>
      rows(i) = pairs(i)._1
      cols(i) = pairs(i)._2
      values(i) = pairs(i)._3
    }
    update(mat, new Increment(mat.id, rows, cols, values))
  }


  /**
    * Assign matrix diagonal with `value`
    */
  def diag(mat: PSMatrix, value: Array[Double]): Unit = {
    mat.assertValid()
    mat.assertCompatible(value)
    assert(mat.columns == mat.rows, s"when init diag matrix, " +
      s"matrix columnNum(${mat.columns}) must equal to rowNum(${mat.rows})")

    update(mat, new Diag(mat.id, value))
  }

  /**
    * Assign matrix diagonal with 1.0
    */
  def eye(mat: PSMatrix): Unit = {
    mat.assertValid()
    assert(mat.columns == mat.rows, s"when init eye matrix, " +
      s"matrix columnNum(${mat.columns}) must equal to rowNum(${mat.rows})")
    update(mat, new Eye(mat.id))

  }

  /**
    * Fill matrix with `value`
    */
  def fill(mat: PSMatrix, value: Double): Unit = {
    mat.assertValid()
    update(mat, new FullFill(mat.id, value))
  }

  /**
   * the following are private methods
   */
  private[spark] def aggregate(matrix: PSMatrix, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(matrix.id, PSContext.getTaskId())
    val result = client.get(func)
    assertSuccess(result)
    result
  }

  private def update(matrix: PSMatrix, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(matrix.id, PSContext.getTaskId())
    val result = client.update(func).get()
    assertSuccess(result)
  }

  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }
}
