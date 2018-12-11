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

import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.matrix.RowBasedMatrix;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.output.format.MatrixFilesMeta;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Matrix loader/saver base class. It define the basic load/save process
 */
public abstract class MatrixLoaderSaverImpl implements MatrixLoaderSaver {

  private final static Log LOG = LogFactory.getLog(MatrixLoaderSaverImpl.class);
  protected final Configuration conf;

  public MatrixLoaderSaverImpl(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void save(Matrix matrix, MatrixSaveContext saveContext, Configuration conf)
      throws IOException {
    throw new UnsupportedOperationException("Unsupport now");
  }

  @Override
  public Matrix load(MatrixLoadContext loadContext, Configuration conf)
      throws IOException {
    LOG.info(
        "load matrix " + loadContext.getMatrixName() + " from path " + loadContext.getLoadPath());
    Path matrixFilesPath = new Path(loadContext.getLoadPath());
    FileSystem fs = matrixFilesPath.getFileSystem(conf);
    if (!fs.exists(matrixFilesPath)) {
      throw new IOException("Can not find matrix file path " + loadContext.getLoadPath());
    }

    // Read matrix meta from meta file
    Path metaFilePath = new Path(matrixFilesPath, ModelFilesConstent.modelMetaFileName);
    if (!fs.exists(matrixFilesPath)) {
      throw new IOException(
          "Can not find meta file for matrix " + loadContext.getMatrixName() + " on path "
              + loadContext.getLoadPath());
    }
    MatrixFilesMeta matrixFilesMeta;
    FSDataInputStream input = fs.open(metaFilePath);
    matrixFilesMeta = new MatrixFilesMeta();
    List<MatrixPartitionMeta> partFileMetas = new ArrayList<>();
    try {
      matrixFilesMeta.read(input);
    } catch (Throwable e) {
      throw new IOException("Read meta failed ", e);
    } finally {
      input.close();
    }
    partFileMetas.addAll(matrixFilesMeta.getPartMetas().values());

    // Sort partitions
    Collections.sort(partFileMetas, new Comparator<MatrixPartitionMeta>() {
      @Override
      public int compare(MatrixPartitionMeta p1, MatrixPartitionMeta p2) {
        if (p1.getFileName().compareTo(p2.getFileName()) < 0) {
          return -1;
        } else if (p1.getFileName().compareTo(p2.getFileName()) > 0) {
          return 1;
        } else {
          return (int) (p1.getOffset() - p2.getOffset());
        }
      }
    });

    int size = partFileMetas.size();
    List<Integer> parts = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      parts.add(partFileMetas.get(i).getPartId());
    }

    long offset;
    String currentFileName = "";
    Matrix matrix = initMatrix(matrixFilesMeta);

    try {
      for (MatrixPartitionMeta partMeta : partFileMetas) {
        String fileName = partMeta.getFileName();
        offset = partMeta.getOffset();
        if (!fileName.equals(currentFileName)) {
          currentFileName = fileName;
          if (input != null) {
            input.close();
          }
          input = fs.open(new Path(loadContext.getLoadPath(), currentFileName));
        }
        input.seek(offset);
        load(matrix, partMeta, loadContext, input);
      }
    } catch (Throwable x) {
      throw new IOException(x);
    } finally {
      if (input != null) {
        input.close();
      }
    }

    return matrix;
  }

  public Matrix initMatrix(MatrixFilesMeta matrixFilesMeta) {
    Map<Integer, MatrixPartitionMeta> partMetas = matrixFilesMeta.getPartMetas();
    Int2LongOpenHashMap rowIdToElemNumMap = new Int2LongOpenHashMap();
    for (MatrixPartitionMeta partMeta : partMetas.values()) {
      Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
      for (Map.Entry<Integer, RowPartitionMeta> rowMetaEntry : rowMetas.entrySet()) {
        rowIdToElemNumMap.addTo(rowMetaEntry.getKey(), rowMetaEntry.getValue().getElementNum());
      }
    }

    RowType rowType = RowType.valueOf(matrixFilesMeta.getRowType());
    RowBasedMatrix matrix = rbMatrix(rowType, matrixFilesMeta.getRow(), matrixFilesMeta.getCol());
    ObjectIterator<Entry> iter = rowIdToElemNumMap.int2LongEntrySet().fastIterator();
    Int2LongMap.Entry entry;
    while (iter.hasNext()) {
      entry = iter.next();
      matrix.setRow(entry.getIntKey(),
          initRow(rowType, matrixFilesMeta.getCol(), entry.getLongValue()));
    }

    return matrix;
  }

  private static RowBasedMatrix rbMatrix(RowType rowType, int rowNum, long colNum) {
    switch (rowType) {
      case T_INT_DENSE:
      case T_INT_SPARSE:
      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        return MFactory.rbIntIntMatrix(rowNum, (int) colNum);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        return MFactory.rbIntLongMatrix(rowNum, (int) colNum);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
        return MFactory.rbIntFloatMatrix(rowNum, (int) colNum);

      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
        return MFactory.rbIntDoubleMatrix(rowNum, (int) colNum);

      case T_INT_SPARSE_LONGKEY:
      case T_INT_DENSE_LONGKEY_COMPONENT:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        return MFactory.rbLongIntMatrix(rowNum, (int) colNum);

      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_DENSE_LONGKEY_COMPONENT:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        return MFactory.rbLongLongMatrix(rowNum, (int) colNum);

      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        return MFactory.rbLongFloatMatrix(rowNum, (int) colNum);

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return MFactory.rbLongDoubleMatrix(rowNum, (int) colNum);

      default:
        throw new UnsupportedOperationException("Unsupport row type " + rowType);
    }
  }

  public static com.tencent.angel.ml.math2.vector.Vector initRow(RowType rowType, long dim,
      long estElemNum) {
    com.tencent.angel.ml.math2.vector.Vector ret;
    switch (rowType) {
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        ret = VFactory.sparseDoubleVector((int) dim, (int) estElemNum);
        break;

      case T_DOUBLE_DENSE:
      case T_DOUBLE_DENSE_COMPONENT:
        ret = VFactory.denseDoubleVector((int) dim);
        break;

      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        ret = VFactory.sparseFloatVector((int) dim, (int) estElemNum);
        break;

      case T_FLOAT_DENSE:
      case T_FLOAT_DENSE_COMPONENT:
        ret = VFactory.denseFloatVector((int) dim);
        break;

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        ret = VFactory.sparseIntVector((int) dim, (int) estElemNum);
        break;

      case T_INT_DENSE:
      case T_INT_DENSE_COMPONENT:
        ret = VFactory.denseIntVector((int) dim);
        break;

      case T_LONG_SPARSE:
      case T_LONG_SPARSE_COMPONENT:
        ret = VFactory.sparseLongVector((int) dim, (int) estElemNum);
        break;

      case T_LONG_DENSE:
      case T_LONG_DENSE_COMPONENT:
        ret = VFactory.denseLongVector((int) dim);
        break;

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        ret = VFactory.sparseLongKeyDoubleVector(dim, (int) estElemNum);
        break;

      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        ret = VFactory.sparseLongKeyFloatVector(dim, (int) estElemNum);
        break;

      case T_INT_SPARSE_LONGKEY:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        ret = VFactory.sparseLongKeyIntVector(dim, (int) estElemNum);
        break;

      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        ret = VFactory.sparseLongKeyLongVector(dim, (int) estElemNum);
        break;

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
        ret = VFactory.denseDoubleVector((int) dim);
        break;

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
        ret = VFactory.denseFloatVector((int) dim);
        break;

      case T_INT_DENSE_LONGKEY_COMPONENT:
        ret = VFactory.denseIntVector((int) dim);
        break;

      case T_LONG_DENSE_LONGKEY_COMPONENT:
        ret = VFactory.denseLongVector((int) dim);
        break;

      default:
        throw new UnsupportedOperationException(
            "can not support " + rowType + " type row for ServerIntDoubleRow");
    }

    return ret;
  }

  public abstract void load(Matrix matrix, MatrixPartitionMeta partMeta,
      MatrixLoadContext loadContext, FSDataInputStream in)
      throws IOException;
}
