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


package com.tencent.angel.psagent.matrix;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;

import java.util.concurrent.Future;

/**
 * Matrix operation interface used by ps client. In addition to the basic operations, we also define
 * the POFs(Ps Oriented Function), which can be used to extend the functionality of the ps.
 */
public interface MatrixInterface {
  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Plus a vector/matrix to the matrix stored in pss
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param rowId row id
   * @param row   the update vector
   * @throws AngelException
   */
  void increment(int rowId, Vector row) throws AngelException;

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param rowId row id
   * @param row   the update vector
   * @throws AngelException
   */
  Future<VoidResult> asyncIncrement(int rowId, Vector row) throws AngelException;

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param row the update vector
   * @throws AngelException
   */
  void increment(Vector row) throws AngelException;

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param row the update vector
   * @throws AngelException
   */
  Future<VoidResult> asyncIncrement(Vector row) throws AngelException;

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param rowId row id
   * @param row   the update vector
   * @throws AngelException
   */
  void increment(int rowId, Vector row, boolean disableCache) throws AngelException;

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param delta        the update vector
   * @param disableCache true means increment to ps directly
   * @throws AngelException
   */
  void increment(Vector delta, boolean disableCache) throws AngelException;


  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param delta the update matrix
   * @throws AngelException
   */
  void increment(Matrix delta) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param delta the update matrix
   * @throws AngelException
   */
  Future<VoidResult> asyncIncrement(Matrix delta) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param delta        the update matrix
   * @param disableCache true means increment to ps directly
   * @throws AngelException
   */
  void increment(Matrix delta, boolean disableCache) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param rowIds the update row ids
   * @param rows   the update rows
   * @throws AngelException
   */
  void increment(int[] rowIds, Vector[] rows) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param rowIds the update row ids
   * @param rows   the update rows
   * @throws AngelException
   */
  Future<VoidResult> asyncIncrement(int[] rowIds, Vector[] rows) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param rowIds       the update row ids
   * @param rows         the update rows
   * @param disableCache true means increment to ps directly
   * @throws AngelException
   */
  void increment(int[] rowIds, Vector[] rows, boolean disableCache) throws AngelException;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Update a vector/matrix to the matrix stored in pss
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param rowId row id
   * @param row   the update vector
   * @throws AngelException
   */
  void update(int rowId, Vector row) throws AngelException;


  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param rowId row id
   * @param row   the update vector
   * @throws AngelException
   */
  Future<VoidResult> asyncUpdate(int rowId, Vector row) throws AngelException;

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param row the update vector
   * @throws AngelException
   */
  void update(Vector row) throws AngelException;

  /**
   * Use a update vector which has same dimension with matrix row to increment the matrix row.
   *
   * @param row the update vector
   * @throws AngelException
   */
  Future<VoidResult> asyncUpdate(Vector row) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param delta the update matrix
   * @throws AngelException
   */
  void update(Matrix delta) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param delta the update matrix
   * @throws AngelException
   */
  Future<VoidResult> asyncUpdate(Matrix delta) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param rowIds the update row ids
   * @param rows   the update rows
   * @throws AngelException
   */
  void update(int[] rowIds, Vector[] rows) throws AngelException;

  /**
   * Use a update matrix which has same dimension with the matrix to increment the matrix.
   *
   * @param rowIds the update row ids
   * @param rows   the update rows
   * @throws AngelException
   */
  Future<VoidResult> asyncUpdate(int[] rowIds, Vector[] rows) throws AngelException;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Get values from pss use row/column indices
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Get elements of the row use int indices, the row type should has "int" type indices
   *
   * @param rowId   row id
   * @param indices elements indices
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  Vector get(int rowId, int[] indices) throws AngelException;

  /**
   * Get elements of the row use int indices, the row type should has "int" type indices
   *
   * @param rowId   row id
   * @param indices elements indices
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  Future<Vector> asyncGet(int rowId, int[] indices) throws AngelException;

  /**
   * Get elements of the row use long indices, the row type should has "int" type indices
   *
   * @param rowId   row id
   * @param indices elements indices
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  Vector get(int rowId, long[] indices) throws AngelException;

  /**
   * Get elements of the row use long indices, the row type should has "int" type indices
   *
   * @param rowId   row id
   * @param indices elements indices
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  Future<Vector> asyncGet(int rowId, long[] indices) throws AngelException;

  /**
   * Get elements of the rows use int indices, the row type should has "int" type indices
   *
   * @param rowIds  rows ids
   * @param indices elements indices
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  Vector[] get(int[] rowIds, int[] indices) throws AngelException;

  /**
   * Get elements of the rows use int indices, the row type should has "int" type indices
   *
   * @param rowIds  rows ids
   * @param indices elements indices
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  Future<Vector[]> asyncGet(int[] rowIds, int[] indices) throws AngelException;

  /**
   * Get elements of the rows use long indices, the row type should has "long" type indices
   *
   * @param rowIds  rows ids
   * @param indices elements indices
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  Vector[] get(int[] rowIds, long[] indices) throws AngelException;

  /**
   * Get elements of the rows use long indices, the row type should has "long" type indices
   *
   * @param rowIds  rows ids
   * @param indices elements indices
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  Future<Vector[]> asyncGet(int[] rowIds, long[] indices) throws AngelException;

  /**
   * Get elements of the row use int indices, if the element does not exist, it will be initialized
   * first, the row type should has "int" type indices
   *
   * @param rowId   row id
   * @param indices elements indices
   * @param func    element initialization method
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  Vector initAndGet(int rowId, int[] indices, InitFunc func) throws AngelException;

  /**
   * Get elements of the row use int indices, if the element does not exist, it will be initialized
   * first, the row type should has "int" type indices
   *
   * @param rowId   row id
   * @param indices elements indices
   * @param func    element initialization method
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  Future<Vector> asyncInitAndGet(int rowId, int[] indices, InitFunc func) throws AngelException;

  /**
   * Get elements of the row use long indices, if the element does not exist, it will be initialized
   * first, the row type should has "int" type indices
   *
   * @param rowId   row id
   * @param indices elements indices
   * @param func    element initialization method
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  Vector initAndGet(int rowId, long[] indices, InitFunc func) throws AngelException;

  /**
   * Get elements of the row use long indices, if the element does not exist, it will be initialized
   * first, the row type should has "int" type indices
   *
   * @param rowId   row id
   * @param indices elements indices
   * @param func    element initialization method
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  Future<Vector> asyncInitAndGet(int rowId, long[] indices, InitFunc func) throws AngelException;

  /**
   * Get elements of the rows use int indices, if the element does not exist, it will be initialized
   * first, the row type should has "int" type indices
   *
   * @param rowIds  rows ids
   * @param indices elements indices
   * @param func    element initialization method
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  Vector[] initAndGet(int[] rowIds, int[] indices, InitFunc func) throws AngelException;

  /**
   * Get elements of the rows use int indices, if the element does not exist, it will be initialized
   * first, the row type should has "int" type indices
   *
   * @param rowIds  rows ids
   * @param indices elements indices
   * @param func    element initialization method
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  Future<Vector[]> asyncInitAndGet(int[] rowIds, int[] indices, InitFunc func) throws AngelException;

  /**
   * Get elements of the rows use long indices, if the element does not exist, it will be initialized
   * first, the row type should has "long" type indices
   *
   * @param rowIds  rows ids
   * @param indices elements indices
   * @param func    element initialization method
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  Vector[] initAndGet(int[] rowIds, long[] indices, InitFunc func) throws AngelException;

  /**
   * Get elements of the rows use long indices, if the element does not exist, it will be initialized
   * first, the row type should has "long" type indices
   *
   * @param rowIds  rows ids
   * @param indices elements indices
   * @param func    element initialization method
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  Future<Vector[]> asyncInitAndGet(int[] rowIds, long[] indices, InitFunc func) throws AngelException;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// PSF get/update, use can implement their own psf
  /////////////////////////////////////////////////////////////////////////////////////////////////

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
   * Use the get function defined by user to get what we need about the matrix. This method use the
   * asynchronous consistency control protocol.
   *
   * @param func the function used to get what we need about the matrix
   * @return GetResult
   * @throws AngelException
   */
  Future<GetResult> asyncGet(GetFunc func) throws AngelException;

  /**
   * Use the update function defined by user to update the matrix.
   *
   * @param func the function used to update the matrix
   * @throws AngelException
   */
  void update(UpdateFunc func) throws AngelException;

  /**
   * Use the update function defined by user to update the matrix.
   *
   * @param func the function used to update the matrix
   * @return Future<VoidResult> the result future, user can choose whether to wait for the update
   * result
   * @throws AngelException
   */
  Future<VoidResult> asyncUpdate(UpdateFunc func) throws AngelException;


  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Get a row or a batch of rows
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Get a matrix row.
   *
   * @param rowId row index
   * @return Vector matrix row
   * @throws AngelException
   */
  Vector getRow(int rowId) throws AngelException;

  /**
   * Get a matrix row.
   *
   * @param rowId row index
   * @return Vector matrix row
   * @throws AngelException
   */
  Future<Vector> asyncGetRow(int rowId) throws AngelException;

  /**
   * Get a matrix row.
   *
   * @param rowId        row index
   * @param disableCache true means get from ps directly
   * @return Vector matrix row
   * @throws AngelException
   */
  Vector getRow(int rowId, boolean disableCache) throws AngelException;


  /**
   * Get a batch of rows use the pipeline mode. The pipeline mode means that user can calculate part
   * of rows while fetch others.
   *
   * @param index     row indexes
   * @param batchSize the batch size return to user once
   * @return GetRowsResult the result which contains a blocking queue
   * @throws AngelException
   */
  GetRowsResult getRowsFlow(RowIndex index, int batchSize) throws AngelException;

  /**
   * Get a batch rows
   * @param rowIds row ids
   * @return rows result
   * @throws AngelException
   */
  Vector [] getRows(int [] rowIds) throws AngelException;

  /**
   * Get a batch rows
   * @param rowIds row ids
   * @param disableCache true means get from ps directly
   * @return rows result
   * @throws AngelException
   */
  Vector [] getRows(int [] rowIds, boolean disableCache) throws AngelException;

  /**
   * Get a batch rows
   * @param rowIds row ids
   * @param batchSize the number of rows in one rpc
   * @return rows result
   * @throws AngelException
   */
  Vector [] getRows(int [] rowIds, int batchSize) throws AngelException;

  /**
   * Get a batch rows
   * @param rowIds row ids
   * @param batchSize the number of rows in one rpc
   * @param disableCache true means get from ps directly
   * @return rows result
   * @throws AngelException
   */
  Vector [] getRows(int [] rowIds, int batchSize, boolean disableCache) throws AngelException;

  /**
   * Get a batch of rows use the pipeline mode. The pipeline mode means that user can calculate part
   * of rows while fetch others.
   *
   * @param disableCache true means get from ps directly
   * @param index        row indexes
   * @param batchSize    the batch size return to user once
   * @param batchSize    the batch size return to user once
   * @return GetRowsResult the result which contains a blocking queue
   * @throws AngelException
   */
  GetRowsResult getRowsFlow(RowIndex index, int batchSize, boolean disableCache)
    throws AngelException;

  /**
   * Update the clock value of the matrix.
   *
   * @param flushFirst true means flush matrix oplogs in cache to ps first
   * @return Future<VoidResult> the result future, user can choose whether to wait for the operation
   * result
   * @throws AngelException
   */
  Future<VoidResult> clock(boolean flushFirst) throws AngelException;

  /**
   * Flush matrix oplogs in cache to ps.
   *
   * @return Future<VoidResult> the result future, user can choose whether to wait for the operation
   * result
   * @throws AngelException
   */
  Future<VoidResult> flush() throws AngelException;

  /**
   * Update the clock value of the matrix.
   *
   * @return Future<VoidResult> the result future, user can choose whether to wait for the operation
   * result
   * @throws AngelException
   */
  Future<VoidResult> clock() throws AngelException;

}
