
package com.tencent.angel.ml.model;

import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.udf.updater.VoidResult;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class PSModel<K extends TVector> {
  private Log LOG = LogFactory.getLog(PSModel.class);

  private MatrixContext matctx;
  private TaskContext ctx;
  private MatrixClient client;

  public PSModel(TaskContext ctx, String name,
                 int row, int col,
                 int blockRow, int blockCol) {
    this(name, row, col, blockRow, blockCol);
    this.ctx = ctx;
  }

  public PSModel(TaskContext ctx, String name,
                 int row, int col) {
    this(name, row, col);
    this.ctx = ctx;
  }

  public PSModel(MatrixContext matctx) {
    this.matctx = matctx;
  }

  public PSModel(String name, int row, int col) {
    matctx = new MatrixContext(name, row, col);
  }

  public PSModel(String name, int row, int col,
                 int blockRow, int blockCol) {
    matctx = new MatrixContext(name, row, col, blockRow, blockCol);
  }

  private MatrixClient getClient() throws Exception {
    if (client == null)
      client = ctx.getMatrix(getName());
    return client;
  }

  public MatrixContext getContext() {
    return matctx;
  }

  public String getName() {
    return matctx.getName();
  }

  public void setAttribute(String key, String value) {
    matctx.set(key, value);
  }

  public K getRow(int rowId) throws Exception {
    return (K) getClient().getRow(rowId);
  }

  public void increment(K delta) throws Exception {
    getClient().increment(delta);
  }

  public void increment(List<K> deltas) throws Exception {
    for (K delta: deltas)
      increment(delta);
  }

  public Future<VoidResult> clock() throws Exception {
    return getClient().clock();
  }

  public Future<VoidResult> clock(boolean flush) throws Exception {
    return getClient().clock(flush);
  }

  public Future<VoidResult> flush() throws Exception {
    return getClient().flush();
  }

  public void zero() throws Exception {
    getClient().zero();
  }

  public GetRowsResult getRowsFlow(RowIndex rowIndex, int batchNum) throws Exception {
    return getClient().getRowsFlow(rowIndex, batchNum);
  }


  public ArrayList<K> getRows(RowIndex rowIndex, int batchNum) throws Exception {
    ArrayList<K> vectors = new ArrayList<>(matctx.getRowNum());
    for (int i = 0; i < matctx.getRowNum(); i++)
      vectors.add(i, null);

    GetRowsResult rows = getClient().getRowsFlow(rowIndex, batchNum);

    while (true) {
      K row = (K) rows.take();
      if (row == null)
        break;
      else
        vectors.set(row.getRowId(), row);
    }
    return vectors;
  }

  public void setAverage(Boolean aver) {
    matctx.set(MatrixConfiguration.MATRIX_AVERAGE, String.valueOf(aver));
  }

  public void setHogwild(Boolean hogwild) {
    matctx.set(MatrixConfiguration.MATRIX_HOGWILD, String.valueOf(hogwild));
  }

  public void setLoadPath(String path) {
    matctx.set(MatrixConfiguration.MATRIX_LOAD_PATH, path);
    LOG.info("Before trainning, matrix " + this.matctx.getName() + " will be loaded from " + path);
  }

  public void setOplogType(String oplogType) {
    matctx.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, oplogType);
  }

  public void setRowType(MLProtos.RowType rowType) {
    matctx.setRowType(rowType);
  }

  public void setSavePath(String path) {
    matctx.set(MatrixConfiguration.MATRIX_SAVE_PATH, path);

    LOG.info("After trainning matrix " + this.matctx.getName() + " will be saved to " + path);
  }
}
