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
package com.tencent.angel.protobuf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helper utility to build protocol buffer responses, or retrieve data from protocol buffer
 * responses.
 */
public final class ResponseConverter {

  static final Log LOG = LogFactory.getLog(ResponseConverter.class);

  private ResponseConverter() {}

  /**
   * static public TVector constructVectorFromGetRowResult(List<GetRowResult> results) {
   * Iterator<GetRowResult> iter = results.iterator(); GetRowResult first = iter.next(); int rowId =
   * first.getRowId(); int matrixId = first.getMatrixId();
   * 
   * // Check the results iter = results.iterator(); while (iter.hasNext()) { GetRowResult result =
   * iter.next(); if (result.getRowId() != rowId || result.getMatrixId() != matrixId) {
   * LOG.error(String.format("construct vector error, rowId or matrixId not match")); return null; }
   * }
   * 
   * // Sort results according to the startCol of partitionKey Collections.sort(results, new
   * Comparator<GetRowResult>() {
   * 
   * @Override public int compare(GetRowResult r1, GetRowResult r2) { return
   *           r1.getPartitionKey().getStartCol() - r2.getPartitionKey().getStartCol(); } });
   * 
   *           iter = results.iterator(); int length = 0; int startCol = Integer.MAX_VALUE; int
   *           endCol = Integer.MIN_VALUE; while (iter.hasNext()) { GetRowResult result =
   *           iter.next(); length += result.numberOfElements(); startCol =
   *           Math.min(result.getStartCol(), startCol); endCol = Math.max(result.getEndCol(),
   *           endCol); }
   * 
   *           // Check startCol and endCol if (startCol != 0 || endCol !=
   *           MLContext.get().getMatrixMetaManager(). getMatrixMeta(matrixId).getColNum()) {
   *           LOG.error(String.format(
   *           "StartCol or EndCol not satisfied,startCol: %d, endCol: %d, server colNum: %d",
   *           startCol, endCol,
   *           MLContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getColNum())); return
   *           null; }
   * 
   *           MLProtos.RowType rowType =
   *           MLContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getRowType(); iter =
   *           results.iterator(); int[] offsets, intValues; double[] doubleValues; switch (rowType)
   *           { case T_DOUBLE_DENSE: doubleValues = new double[endCol]; while (iter.hasNext()) {
   *           GetRowResult result = iter.next(); result.convertSplit(doubleValues); } return new
   *           DenseDoubleVector(endCol, doubleValues); case T_DOUBLE_SPARSE: offsets = new
   *           int[length]; doubleValues = new double[length]; int start = 0; while (iter.hasNext())
   *           { GetRowResult result = iter.next(); result.convertSplit(start, offsets,
   *           doubleValues); start += result.numberOfElements(); } return new
   *           SparseDoubleVector(endCol, offsets, doubleValues); case T_INT_DENSE: intValues = new
   *           int[endCol]; while (iter.hasNext()) { iter.next().convertSplit(intValues); } return
   *           new DenseIntVector(endCol, intValues); case T_INT_SPARSE: return null; default:
   *           return null; } }
   */
  /*
   * static public TVector constructVectorFromRowSplit(List<RowSplit> results) throws
   * UnvalidRowSplitException { Iterator<RowSplit> iter = results.iterator(); RowSplit first =
   * iter.next(); int rowId = first.getRowId(); int matrixId =
   * first.getPartitionKey().getMatrixId(); int clock = first.getClock();
   * 
   * int length = 0; int startCol = Integer.MAX_VALUE; int endCol = Integer.MIN_VALUE;
   * 
   * TVector ret = null;
   * 
   * iter = results.iterator();
   * 
   * while (iter.hasNext()) { RowSplit result = iter.next(); if (result.getRowId() != rowId ||
   * result.getPartitionKey().getMatrixId() != matrixId) { throw new
   * UnvalidRowSplitException(String.format("construct vector error, rowId or matrixId not match" +
   * ", result rowid = %d, need rowid = %d, result matrixid= %d, need matrixid = %d" ,
   * result.getRowId(), rowId, result.getPartitionKey().getMatrixId(), matrixId)); }
   * 
   * length += result.numberOfElements(); startCol =
   * Math.min(result.getPartitionKey().getStartCol(), startCol); endCol =
   * Math.max(result.getPartitionKey().getEndCol(), endCol); }
   * 
   * // Check startCol and endCol if (startCol != 0 || endCol !=
   * MLContext.get().getMatrixMetaManager(). getMatrixMeta(matrixId).getColNum()) { throw new
   * UnvalidRowSplitException
   * (String.format("StartCol or EndCol not satisfied,startCol: %d, endCol: %d, server colNum: %d",
   * startCol, endCol, MLContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getColNum()));
   * }
   * 
   * // Sort results according to the startCol of partitionKey Collections.sort(results, new
   * Comparator<RowSplit>() {
   * 
   * @Override public int compare(RowSplit r1, RowSplit r2) { return
   * r1.getPartitionKey().getStartCol() - r2.getPartitionKey().getStartCol(); } });
   * 
   * MLProtos.RowType rowType =
   * MLContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getRowType(); ret =
   * buildTVector(rowType, results, endCol, length); ret.setClock(clock); ret.setMatrixId(matrixId);
   * ret.setRowId(rowId); return ret; }
   * 
   * 
   * public static TVector buildTVector(MLProtos.RowType localType, List<RowSplit> list, int dim,
   * int length) throws UnvalidRowSplitException { TVector ret; switch (localType) { case
   * T_DOUBLE_DENSE: ret = buildDenseDoubleVector(list, dim); break; case T_DOUBLE_SPARSE: ret =
   * buildSparseDoubleVector(list, dim, length); break;
   * 
   * case T_INT_DENSE: ret = buildDenseIntVector(list, dim); break;
   * 
   * case T_INT_ARBITRARY: ret = buildTIntVector(list, dim, length); break;
   * 
   * default: throw new UnvalidRowSplitException("unsupport row type " + localType); } return ret; }
   * 
   * public static DenseDoubleVector buildDenseDoubleVector(List<RowSplit> list, int dim) { double[]
   * values = new double[dim]; Iterator<RowSplit> iter = list.iterator(); while (iter.hasNext()) {
   * DoubleRowSplit result = (DoubleRowSplit) iter.next(); result.fill(values); } return new
   * DenseDoubleVector(dim, values); }
   * 
   * public static SparseDoubleVector buildSparseDoubleVector(List<RowSplit> list, int dim, int
   * length) { int[] keys = new int[length]; double[] values = new double[length]; int start = 0;
   * Iterator<RowSplit> iter = list.iterator(); while (iter.hasNext()) { DoubleRowSplit result =
   * (DoubleRowSplit) iter.next(); result.fill(start, keys, values); start +=
   * result.numberOfElements(); } return new SparseDoubleVector(dim, keys, values); }
   * 
   * public static DenseIntVector buildDenseIntVector(List<RowSplit> list, int dim) { int[] values =
   * new int[dim]; Iterator<RowSplit> iter = list.iterator(); while (iter.hasNext()) {
   * ((IntRowSplit)iter.next()).fill(values); } return new DenseIntVector(dim, values); }
   * 
   * public static SparseIntVector buildSparseIntVector(List<RowSplit> list, int dim, int length) {
   * SparseIntVector vector = new SparseIntVector(dim, length); Iterator<RowSplit> iter =
   * list.iterator(); while (iter.hasNext()) { ((IntRowSplit) iter.next()).fill(vector); } return
   * vector; }
   * 
   * public static SparseIntSortedVector buildSparseIntSortedVector(List<RowSplit> list, int dim,
   * int length) { int[] keys = new int[length]; int[] values = new int[length]; Iterator<RowSplit>
   * iter = list.iterator(); int start = 0; while (iter.hasNext()) { IntRowSplit result =
   * (IntRowSplit) iter.next(); result.fill(start, keys, values); start +=
   * result.numberOfElements(); } return new SparseIntSortedVector(dim, keys, values); }
   * 
   * public static TIntVector buildTIntVector(List<RowSplit> list, int dim, int length) { if (length
   * < dim * 0.5) return buildSparseIntVector(list, dim, length); return buildDenseIntVector(list,
   * dim); }
   */
}
