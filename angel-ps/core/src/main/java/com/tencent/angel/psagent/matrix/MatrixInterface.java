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
 */

package com.tencent.angel.psagent.matrix;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;

import java.util.concurrent.Future;

/**
 * Matrix operation interface used by ps client. In addition to the basic operations, we also define
 * the POFs(Ps Oriented Function), which can be used to extend the functionality of the ps.
 */
public interface MatrixInterface {
  /**
   * Use the update function defined by user to update the matrix.
   * 
   * @param func the function used to update the matrix
   * @return Future<VoidResult> the result future, user can choose whether to wait for the update
   *         result
   * @throws AngelException
   */
  Future<VoidResult> update(UpdateFunc func) throws AngelException;

  /**
   * Use the get function defined by user to get what we need about the matrix. This method use the
   * asynchronous consistency control protocol.
   * 
   * @param func the function used to get what we need about the matrix
   * @return GetResult
   * @throws AngelException
   */
  GetResult get(GetFunc func) throws AngelException;


  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   * 
   * @param rowIndex row index
   * @param delta the update vector
   * @throws AngelException
   */
  void increment(int rowIndex, TVector delta) throws AngelException;

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   * 
   * @param delta the update vector
   * @throws AngelException
   */
  void increment(TVector delta) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   * 
   * @param delta the update matrix
   * @throws AngelException
   */
  void increment(TMatrix delta) throws AngelException;

  /**
   * Get a matrix row.
   * 
   * @param rowIndex row index
   * @return TVector matrix row
   * @throws AngelException
   */
  TVector getRow(int rowIndex) throws AngelException;

  /**
   * Get a batch of rows use the pipeline mode. The pipeline mode means that user can calculate part
   * of rows while fetch others.
   * 
   * @param index row indexes
   * @param batchSize the batch size return to user once
   * @return GetRowsResult the result which contains a blocking queue
   * @throws AngelException
   */
  GetRowsResult getRowsFlow(RowIndex index, int batchSize) throws AngelException;

  /**
   * Update the clock value of the matrix.
   * 
   * @param flushFirst true means flush matrix oplogs in cache to ps first
   * @return Future<VoidResult> the result future, user can choose whether to wait for the operation
   *         result
   * @throws AngelException
   */
  Future<VoidResult> clock(boolean flushFirst) throws AngelException;

  /**
   * Flush matrix oplogs in cache to ps.
   * 
   * @return Future<VoidResult> the result future, user can choose whether to wait for the operation
   *         result
   * @throws AngelException
   */
  Future<VoidResult> flush() throws AngelException;

  /**
   * Update the clock value of the matrix.
   * 
   * @return Future<VoidResult> the result future, user can choose whether to wait for the operation
   *         result
   * @throws AngelException
   */
  Future<VoidResult> clock() throws AngelException;
}
