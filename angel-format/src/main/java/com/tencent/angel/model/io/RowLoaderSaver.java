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

import com.tencent.angel.ml.servingmath2.matrix.Matrix;
import com.tencent.angel.ml.servingmath2.vector.IntDoubleVector;
import com.tencent.angel.ml.servingmath2.vector.IntFloatVector;
import com.tencent.angel.ml.servingmath2.vector.IntIntVector;
import com.tencent.angel.ml.servingmath2.vector.IntLongVector;
import com.tencent.angel.ml.servingmath2.vector.LongDoubleVector;
import com.tencent.angel.ml.servingmath2.vector.LongFloatVector;
import com.tencent.angel.ml.servingmath2.vector.LongIntVector;
import com.tencent.angel.ml.servingmath2.vector.LongLongVector;
import com.tencent.angel.ml.servingmath2.vector.Vector;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowFormat;
import com.tencent.angel.model.output.format.RowFormatContext;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Loader/Saver use row-base matrix
 */
public class RowLoaderSaver extends MatrixLoaderSaverImpl {

  private final static Log LOG = LogFactory.getLog(RowLoaderSaver.class);
  private final RowFormat format;

  public RowLoaderSaver(RowFormat format, Configuration conf) {
    super(conf);
    this.format = format;
  }

  public void load(Matrix matrix, MatrixPartitionMeta partMeta, MatrixLoadContext loadContext,
      FSDataInputStream in) throws IOException {
    Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
    for (int rowId : rowMetas.keySet()) {
      Vector row = matrix.getRow(rowId);
      load(row, partMeta, loadContext, in);
    }
  }

  public void load(Vector row, MatrixPartitionMeta meta, MatrixLoadContext loadContext,
      DataInputStream in) throws IOException {
    try {
      if (row instanceof IntFloatVector) {
        load((IntFloatVector) row, loadContext, meta, in);
      } else if (row instanceof IntDoubleVector) {
        load((IntDoubleVector) row, loadContext, meta, in);
      } else if (row instanceof IntIntVector) {
        load((IntIntVector) row, loadContext, meta, in);
      } else if (row instanceof IntLongVector) {
        load((IntLongVector) row, loadContext, meta, in);
      } else if (row instanceof LongFloatVector) {
        load((LongFloatVector) row, loadContext, meta, in);
      } else if (row instanceof LongDoubleVector) {
        load((LongDoubleVector) row, loadContext, meta, in);
      } else if (row instanceof LongIntVector) {
        load((LongIntVector) row, loadContext, meta, in);
      } else if (row instanceof LongLongVector) {
        load((LongLongVector) row, loadContext, meta, in);
      } else {
        throw new IOException("Unknown vector type " + row.getType());
      }
    } finally {
    }
  }

  private void load(IntFloatVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }

    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load(row, in, context);
  }

  private void load(IntDoubleVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load(row, in, context);
  }

  private void load(IntIntVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load(row, in, context);
  }

  private void load(IntLongVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load(row, in, context);
  }

  private void load(LongFloatVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load(row, in, context);
  }

  private void load(LongDoubleVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load(row, in, context);
  }

  private void load(LongIntVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load(row, in, context);
  }

  private void load(LongLongVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    RowFormatContext context = new RowFormatContext();
    context.offset = meta.getStartCol();
    context.useIntIndex = false;
    context.elementNum = rowMeta.getElementNum();

    format.load(row, in, context);
  }
}
