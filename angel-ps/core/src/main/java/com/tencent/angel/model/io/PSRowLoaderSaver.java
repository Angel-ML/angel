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

package com.tencent.angel.model.io;

import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowFormat;
import com.tencent.angel.model.output.format.RowFormatContext;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import com.tencent.angel.ps.storage.matrix.PartitionSource;
import com.tencent.angel.ps.storage.matrix.PartitionState;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerIntLongRow;
import com.tencent.angel.ps.storage.vector.ServerLongDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerLongFloatRow;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import com.tencent.angel.ps.storage.vector.ServerLongLongRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * Loader/Saver for column-base matrix, it can only be used in PS
 */
public class PSRowLoaderSaver extends PSMatrixLoaderSaverImpl {

  private final static Log LOG = LogFactory.getLog(PSRowLoaderSaver.class);
  private final RowFormat format;

  public PSRowLoaderSaver(RowFormat format, Configuration conf) {
    super(conf);
    this.format = format;
  }

  /**
   * Matrix partition data
   *
   * @param part matrix partition
   * @param partMeta matrix partition data meta
   * @param saveContext save context
   * @param output output stream
   */
  public void save(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    List<Integer> rowIds = saveContext.getRowIndexes();
    PartitionSource rows = part.getRows();
    if (rowIds == null || rowIds.isEmpty()) {
      Iterator<Entry<Integer, ServerRow>> iter = part.getRows().iterator();
      rowIds = new ArrayList<>();
      while (iter.hasNext()) {
        rowIds.add(iter.next().getKey());
      }
    } else {
      rowIds = filter(part, rowIds);
    }

    FSDataOutputStream dataOutputStream =
        new FSDataOutputStream(output, null, partMeta != null ? partMeta.getOffset() : 0);

    partMeta.setSaveRowNum(rowIds.size());
    for (int rowId : rowIds) {
      ServerRow row = rows.getRow(rowId);
      RowPartitionMeta rowMeta = new RowPartitionMeta(rowId, 0, 0);
      if (row != null) {
        rowMeta.setElementNum(row.size());
        rowMeta.setOffset(dataOutputStream.getPos());
        save(rows.getRow(rowId), saveContext, partMeta, output);
      } else {
        rowMeta.setElementNum(0);
        rowMeta.setOffset(dataOutputStream.getPos());
      }
      partMeta.setRowMeta(rowMeta);
    }
  }

  /**
   * Save a row
   *
   * @param row row data
   * @param saveContext save context
   * @param meta partition data meta
   * @param out output stream
   */
  public void save(ServerRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    if (saveContext.cloneFirst()) {
      row = row.clone();
    }
    try {
      row.startWrite();
      if (row instanceof ServerIntFloatRow) {
        save((ServerIntFloatRow) row, saveContext, meta, out);
      } else if (row instanceof ServerIntDoubleRow) {
        save((ServerIntDoubleRow) row, saveContext, meta, out);
      } else if (row instanceof ServerIntIntRow) {
        save((ServerIntIntRow) row, saveContext, meta, out);
      } else if (row instanceof ServerIntLongRow) {
        save((ServerIntLongRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongFloatRow) {
        save((ServerLongFloatRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongDoubleRow) {
        save((ServerLongDoubleRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongIntRow) {
        save((ServerLongIntRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongLongRow) {
        save((ServerLongLongRow) row, saveContext, meta, out);
      } else {
        throw new IOException("Unknown vector type " + row.getRowType());
      }
    } finally {
      row.endWrite();
    }
  }

  /**
   * Load a matrix partition
   *
   * @param part matrix partition
   * @param partMeta matrix partition data meta
   * @param loadContext load context
   * @param input input stream
   */
  public void load(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    PartitionSource rows = part.getRows();
    try {
      Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
      for (RowPartitionMeta rowMeta : rowMetas.values()) {
        ServerRow row = rows.getRow(rowMeta.getRowId());
        load(row, partMeta, loadContext, input);
      }
    } finally {
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }

  /**
   * Load a row data
   *
   * @param row row partition
   * @param meta partition meta
   * @param loadContext load context
   * @param in input stream
   */
  public void load(ServerRow row, MatrixPartitionMeta meta, PSMatrixLoadContext loadContext,
      DataInputStream in) throws IOException {
    try {
      row.startWrite();
      if (row instanceof ServerIntFloatRow) {
        load((ServerIntFloatRow) row, loadContext, meta, in);
      } else if (row instanceof ServerIntDoubleRow) {
        load((ServerIntDoubleRow) row, loadContext, meta, in);
      } else if (row instanceof ServerIntIntRow) {
        load((ServerIntIntRow) row, loadContext, meta, in);
      } else if (row instanceof ServerIntLongRow) {
        load((ServerIntLongRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongFloatRow) {
        load((ServerLongFloatRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongDoubleRow) {
        load((ServerLongDoubleRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongIntRow) {
        load((ServerLongIntRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongLongRow) {
        load((ServerLongLongRow) row, loadContext, meta, in);
      } else {
        throw new IOException("Unknown vector type " + row.getRowType());
      }
    } finally {
      row.endWrite();
    }
  }

  private void save(ServerIntFloatRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.sortFirst = saveContext.sortFirst();
    context.useIntIndex = true;
    format.save((IntFloatVector) row.getSplit(), out, context);
  }

  private void save(ServerIntDoubleRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.sortFirst = saveContext.sortFirst();
    context.useIntIndex = true;
    format.save((IntDoubleVector) row.getSplit(), out, context);
  }

  private void save(ServerIntIntRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.sortFirst = saveContext.sortFirst();
    context.useIntIndex = true;
    format.save((IntIntVector) row.getSplit(), out, context);
  }

  private void save(ServerIntLongRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.sortFirst = saveContext.sortFirst();
    context.useIntIndex = true;
    format.save((IntLongVector) row.getSplit(), out, context);
  }

  private void save(ServerLongDoubleRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.sortFirst = saveContext.sortFirst();
    context.useIntIndex = false;

    if (row.getSplit() instanceof IntDoubleVector) {
      format.save((IntDoubleVector) row.getSplit(), out, context);
    } else {
      format.save((LongDoubleVector) row.getSplit(), out, context);
    }
  }

  private void save(ServerLongFloatRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.sortFirst = saveContext.sortFirst();
    context.useIntIndex = false;

    if (row.getSplit() instanceof IntDoubleVector) {
      format.save((IntFloatVector) row.getSplit(), out, context);
    } else {
      format.save((LongFloatVector) row.getSplit(), out, context);
    }
  }


  private void save(ServerLongIntRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.sortFirst = saveContext.sortFirst();
    context.useIntIndex = false;

    if (row.getSplit() instanceof IntDoubleVector) {
      format.save((IntIntVector) row.getSplit(), out, context);
    } else {
      format.save((LongIntVector) row.getSplit(), out, context);
    }
  }

  private void save(ServerLongLongRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.sortFirst = saveContext.sortFirst();
    context.useIntIndex = false;

    if (row.getSplit() instanceof IntDoubleVector) {
      format.save((IntLongVector) row.getSplit(), out, context);
    } else {
      format.save((LongLongVector) row.getSplit(), out, context);
    }
  }

  private void load(ServerIntFloatRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load((IntFloatVector) row.getSplit(), in, context);
  }

  private void load(ServerIntDoubleRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load((IntDoubleVector) row.getSplit(), in, context);
  }

  private void load(ServerIntIntRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load((IntIntVector) row.getSplit(), in, context);
  }

  private void load(ServerIntLongRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load((IntLongVector) row.getSplit(), in, context);
  }

  private void load(ServerLongFloatRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    if (row.getSplit() instanceof IntFloatVector) {
      format.load((IntFloatVector) row.getSplit(), in, context);
    } else {
      format.load((LongFloatVector) row.getSplit(), in, context);
    }
  }

  private void load(ServerLongDoubleRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    if (row.getSplit() instanceof IntDoubleVector) {
      format.load((IntDoubleVector) row.getSplit(), in, context);
    } else {
      format.load((LongDoubleVector) row.getSplit(), in, context);
    }
  }

  private void load(ServerLongIntRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    if (row.getSplit() instanceof IntIntVector) {
      format.load((IntIntVector) row.getSplit(), in, context);
    } else {
      format.load((LongIntVector) row.getSplit(), in, context);
    }
  }

  private void load(ServerLongLongRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    if (row.getSplit() instanceof IntLongVector) {
      format.load((IntLongVector) row.getSplit(), in, context);
    } else {
      format.load((LongLongVector) row.getSplit(), in, context);
    }
  }
}
