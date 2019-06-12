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

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.output.element.IntDoubleElement;
import com.tencent.angel.model.output.element.IntFloatElement;
import com.tencent.angel.model.output.element.IntIntElement;
import com.tencent.angel.model.output.element.IntLongElement;
import com.tencent.angel.model.output.element.LongDoubleElement;
import com.tencent.angel.model.output.element.LongFloatElement;
import com.tencent.angel.model.output.element.LongIntElement;
import com.tencent.angel.model.output.element.LongLongElement;
import com.tencent.angel.model.output.format.ElementFormat;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Loader/Saver use row-base matrix, it masks the details of all row vector traversal
 */
public class RowElementLoaderSaver extends MatrixLoaderSaverImpl {

  private final static Log LOG = LogFactory.getLog(RowElementLoaderSaver.class);

  /**
   * Element format
   */
  private final ElementFormat format;

  public RowElementLoaderSaver(ElementFormat format, Configuration conf) {
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
    int elemNum = rowMeta.getElementNum();
    IntFloatElement element = new IntFloatElement();
    for (int i = 0; i < elemNum; i++) {
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(IntDoubleVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    IntDoubleElement element = new IntDoubleElement();
    for (int i = 0; i < elemNum; i++) {
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(IntIntVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    IntIntElement element = new IntIntElement();
    for (int i = 0; i < elemNum; i++) {
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(IntLongVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    IntLongElement element = new IntLongElement();
    for (int i = 0; i < elemNum; i++) {
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(LongFloatVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    LongFloatElement element = new LongFloatElement();
    for (int i = 0; i < elemNum; i++) {
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(LongDoubleVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    LongDoubleElement element = new LongDoubleElement();
    for (int i = 0; i < elemNum; i++) {
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(LongIntVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    LongIntElement element = new LongIntElement();
    for (int i = 0; i < elemNum; i++) {
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(LongLongVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if (rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    LongLongElement element = new LongLongElement();
    for (int i = 0; i < elemNum; i++) {
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }
}
