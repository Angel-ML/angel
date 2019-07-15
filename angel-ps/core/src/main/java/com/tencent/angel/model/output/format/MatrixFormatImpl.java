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

package com.tencent.angel.model.output.format;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.matrix.RowBasedMatrix;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.ps.storage.matrix.PartitionSource;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.utils.HdfsUtil;
import com.tencent.angel.utils.StringUtils;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.Vector;
import java.util.concurrent.RecursiveAction;

/**
 * Default matrix format implement.
 */
public abstract class MatrixFormatImpl implements MatrixFormat {
  private final static Log LOG = LogFactory.getLog(MatrixFormatImpl.class);
  protected final Configuration conf;

  public MatrixFormatImpl(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Save a matrix partition
   *
   * @param part        matrix partition
   * @param partMeta    matrix partition data meta
   * @param saveContext save context
   * @param output      output stream
   * @throws IOException
   */
  public abstract void save(ServerPartition part, MatrixPartitionMeta partMeta,
    PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException;

  /**
   * Load a matrix partition
   *
   * @param part        matrix partition
   * @param partMeta    matrix partition data meta
   * @param loadContext load context
   * @param input       input stream
   * @throws IOException
   */
  public abstract void load(ServerPartition part, MatrixPartitionMeta partMeta,
    PSMatrixLoadContext loadContext, DataInputStream input) throws IOException;

  @Override
  public void save(ServerMatrix matrix, PSMatrixSaveContext saveContext, Configuration conf)
    throws IOException {
    Path matrixFilesPath = new Path(saveContext.getSavePath());
    FileSystem fs = matrixFilesPath.getFileSystem(conf);
    if (!fs.mkdirs(matrixFilesPath)) {
      String errorMsg =
        "can not create output path " + matrixFilesPath + " for matrix " + matrix.getName();
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }

    LOG.info("Commit partitions of matrix " + matrix.getName() + " to path " + matrixFilesPath);

    // Save partitions to files use fork-join
    PSMatrixFilesMeta psMatrixFilesMeta = new PSMatrixFilesMeta(matrix.getId());

    List<Integer> partIds = new ArrayList<>(saveContext.getPartIds());
    Collections.sort(partIds, new Comparator<Integer>() {
      @Override public int compare(Integer id1, Integer id2) {
        return id1 - id2;
      }
    });

    Vector<String> errorLogs = new Vector<>();
    int maxPartsInSingleFile = conf.getInt(AngelConf.ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE,
      AngelConf.DEFAULT_ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE);
    PartitionDiskOp commitOp =
      new PartitionDiskOp(matrix, fs, matrixFilesPath, ACTION.SAVE, partIds, saveContext,
        psMatrixFilesMeta, errorLogs, 0, partIds.size(), maxPartsInSingleFile);
    saveContext.getWorkers().execute(commitOp);
    commitOp.join();

    if (!errorLogs.isEmpty()) {
      throw new IOException(StringUtils.join("\n", errorLogs));
    }

    // Write the ps matrix meta to the meta file
    Path metaFile = new Path(matrixFilesPath, ModelFilesConstent.psModelMetaFileName);
    Path tmpMetaFile = HdfsUtil.toTmpPath(metaFile);
    FSDataOutputStream metaOut = null;
    try {
      metaOut = fs.create(tmpMetaFile);
      psMatrixFilesMeta.write(metaOut);
      metaOut.flush();
    } finally {
      if(metaOut != null) {
        metaOut.close();
      }
    }

    LOG.info("Rename from " + tmpMetaFile.toString() + " to " + metaFile);
    HdfsUtil.rename(tmpMetaFile, metaFile, fs);
  }

  @Override
  public void load(ServerMatrix matrix, PSMatrixLoadContext loadContext, Configuration conf)
    throws IOException {
    LOG.info("load matrix " + matrix.getName() + " from path " + loadContext.getLoadPath());
    Path matrixFilesPath = new Path(loadContext.getLoadPath());
    FileSystem fs = matrixFilesPath.getFileSystem(conf);
    if (!fs.exists(matrixFilesPath)) {
      LOG.error(
        "Can not find matrix " + matrix.getName() + " in directory " + loadContext.getLoadPath());
      throw new IOException("Can not find matrix " + matrix.getName() + " in directory " + loadContext.getLoadPath());
      //matrix.startServering();
      //return;
    }

    // Read matrix meta from meta file
    PSMatrixFilesMeta psMatrixFilesMeta = new PSMatrixFilesMeta(matrix.getId());
    Map<Integer, ServerPartition> partitionMaps = matrix.getPartitions();
    List<MatrixPartitionMeta> partFileMetas = new ArrayList<>(partitionMaps.size());
    Path metaFilePath = new Path(matrixFilesPath, ModelFilesConstent.modelMetaFileName);
    if (fs.exists(metaFilePath)) {
      FSDataInputStream input = fs.open(metaFilePath);
      MatrixFilesMeta matrixFilesMeta = new MatrixFilesMeta();
      try {
        matrixFilesMeta.read(input);
      } catch (Throwable e) {
        throw new IOException("Read meta failed ", e);
      } finally {
        input.close();
      }

      Map<Integer, MatrixPartitionMeta> partIdToFileMetaMap = matrixFilesMeta.getPartMetas();
      for (int partId : partitionMaps.keySet()) {
        partFileMetas.add(partIdToFileMetaMap.get(partId));
        psMatrixFilesMeta.addPartitionMeta(partId, partIdToFileMetaMap.get(partId));
      }
    } else {
      Path psMetaFilePath = new Path(matrixFilesPath, ModelFilesConstent.psModelMetaFileName);
      if (fs.exists(psMetaFilePath)) {
        FSDataInputStream input = fs.open(psMetaFilePath);
        try {
          psMatrixFilesMeta.read(input);
        } catch (Throwable e) {
          throw new IOException("Read meta failed ", e);
        } finally {
          input.close();
        }

        Map<Integer, MatrixPartitionMeta> partIdToFileMetaMap = psMatrixFilesMeta.getPartMetas();
        for (int partId : partitionMaps.keySet()) {
          partFileMetas.add(partIdToFileMetaMap.get(partId));
          psMatrixFilesMeta.addPartitionMeta(partId, partIdToFileMetaMap.get(partId));
        }
      } else {
        LOG.warn("Can not find matrix meta file in directory " + loadContext.getLoadPath());
        matrix.startServering();
        return;
      }
    }

    Collections.sort(partFileMetas, new Comparator<MatrixPartitionMeta>() {
      @Override public int compare(MatrixPartitionMeta p1, MatrixPartitionMeta p2) {
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

    // Load partitions from file use fork-join
    Vector<String> errorLogs = new Vector<>();
    int maxPartsInSingleFile = conf.getInt(AngelConf.ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE,
      AngelConf.DEFAULT_ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE);
    PartitionDiskOp loadOp =
      new PartitionDiskOp(matrix, fs, matrixFilesPath, ACTION.LOAD, parts, loadContext,
        psMatrixFilesMeta, errorLogs, 0, parts.size(), maxPartsInSingleFile);
    loadContext.getWorkers().execute(loadOp);
    loadOp.join();
    if (!errorLogs.isEmpty()) {
      String errorLog =
        "load partitions for matrix " + matrix.getName() + " failed, error log is " + StringUtils
          .join("\n", errorLogs);
      LOG.error(errorLog);
      throw new IOException(errorLog);
    }
  }

  @Override public void save(Matrix matrix, MatrixSaveContext saveContext, Configuration conf)
    throws IOException {
    throw new UnsupportedOperationException("Unsupport now");
  }

  @Override public Matrix load(MatrixLoadContext loadContext, Configuration conf)
    throws IOException {
    LOG.info("load matrix " + loadContext.getMatrixName() + " from path " + loadContext.getLoadPath());
    Path matrixFilesPath = new Path(loadContext.getLoadPath());
    FileSystem fs = matrixFilesPath.getFileSystem(conf);
    if (!fs.exists(matrixFilesPath)) {
      throw new IOException("Can not find matrix file path " + loadContext.getLoadPath());
    }

    // Read matrix meta from meta file
    Path metaFilePath = new Path(matrixFilesPath, ModelFilesConstent.modelMetaFileName);
    if(!fs.exists(matrixFilesPath)) {
      throw new IOException("Can not find meta file for matrix " + loadContext.getMatrixName() + " on path " + loadContext.getLoadPath());
    }
    MatrixFilesMeta matrixFilesMeta;
    fs.setVerifyChecksum(false);
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
      @Override public int compare(MatrixPartitionMeta p1, MatrixPartitionMeta p2) {
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
          fs.setVerifyChecksum(false);
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
    for(MatrixPartitionMeta partMeta : partMetas.values()) {
      Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
      for(Map.Entry<Integer, RowPartitionMeta> rowMetaEntry : rowMetas.entrySet()) {
        rowIdToElemNumMap.addTo(rowMetaEntry.getKey(), rowMetaEntry.getValue().getElementNum());
      }
    }

    RowType rowType = RowType.valueOf(matrixFilesMeta.getRowType());
    RowBasedMatrix matrix = rbMatrix(rowType, matrixFilesMeta.getRow(), matrixFilesMeta.getCol());
    ObjectIterator<Int2LongMap.Entry> iter = rowIdToElemNumMap.int2LongEntrySet().fastIterator();
    Int2LongMap.Entry entry;
    while(iter.hasNext()) {
      entry = iter.next();
      matrix.setRow(entry.getIntKey(), initRow(rowType, matrixFilesMeta.getCol(), entry.getLongValue()));
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

  public static com.tencent.angel.ml.math2.vector.Vector initRow(RowType rowType, long dim, long estElemNum) {
    com.tencent.angel.ml.math2.vector.Vector ret;
    switch (rowType) {
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        ret = VFactory.sparseDoubleVector((int) dim, (int)estElemNum);
        break;

      case T_DOUBLE_DENSE:
      case T_DOUBLE_DENSE_COMPONENT:
        ret = VFactory.denseDoubleVector((int) dim);
        break;

      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        ret = VFactory.sparseFloatVector((int) dim, (int)estElemNum);
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

  public abstract void load(Matrix matrix, MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, FSDataInputStream in)
    throws IOException;

  enum ACTION {
    LOAD, SAVE
  }

  class PartitionDiskOp extends RecursiveAction {
    private final ServerMatrix matrix;
    private final Path matrixPath;
    private final FileSystem fs;
    private final List<Integer> partIds;
    private final Object context;
    private final PSMatrixFilesMeta dataFilesMeta;
    private final java.util.Vector<String> errorMsgs;
    private final int startPos;
    private final int endPos;
    private final ACTION action;
    private final int maxPartsInSingleFile;

    public PartitionDiskOp(ServerMatrix matrix, FileSystem fs, Path matrixPath, ACTION action,
      List<Integer> partIds, Object context, PSMatrixFilesMeta dataFilesMeta,
      Vector<String> errorMsgs, int startPos, int endPos, int maxPartsInSingleFile) {
      this.matrix = matrix;
      this.fs = fs;
      this.matrixPath = matrixPath;
      this.action = action;
      this.partIds = partIds;
      this.context = context;
      this.dataFilesMeta = dataFilesMeta;
      this.errorMsgs = errorMsgs;
      this.startPos = startPos;
      this.endPos = endPos;
      this.maxPartsInSingleFile = maxPartsInSingleFile;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos <= maxPartsInSingleFile) {
        try {
          process(matrix, matrixPath, fs, action, partIds, context, startPos, endPos,
            dataFilesMeta);
        } catch (Throwable x) {
          LOG.error(action + " model partitions failed.", x);
          errorMsgs.add(action + " model partitions failed." + x.getMessage());
        }
      } else {
        int middle = (startPos + endPos) / 2;
        PartitionDiskOp opLeft =
          new PartitionDiskOp(matrix, fs, matrixPath, action, partIds, context, dataFilesMeta,
            errorMsgs, startPos, middle, maxPartsInSingleFile);
        PartitionDiskOp opRight =
          new PartitionDiskOp(matrix, fs, matrixPath, action, partIds, context, dataFilesMeta,
            errorMsgs, middle, endPos, maxPartsInSingleFile);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private void process(ServerMatrix matrix, Path matrixPath, FileSystem fs, ACTION action,
    List<Integer> partIds, Object context, int startPos, int endPos,
    PSMatrixFilesMeta serverMatrixMeta) throws IOException {
    switch (action) {
      case SAVE:
        save(matrix, matrixPath, fs, partIds, (PSMatrixSaveContext) context, startPos, endPos,
          serverMatrixMeta);
        break;

      case LOAD:
        load(matrix, matrixPath, fs, partIds, (PSMatrixLoadContext) context, startPos, endPos,
          serverMatrixMeta);
        break;

      default:
        break;
    }
  }

  private void save(ServerMatrix matrix, Path matrixPath, FileSystem fs, List<Integer> partIds,
    PSMatrixSaveContext saveContext, int startPos, int endPos, PSMatrixFilesMeta dataFilesMeta)
    throws IOException {

    Path destFile = new Path(matrixPath, ModelFilesUtils.fileName(partIds.get(startPos)));
    Path tmpDestFile = HdfsUtil.toTmpPath(destFile);

    FSDataOutputStream out = fs.create(tmpDestFile);
    long streamPos = 0;
    ServerPartition partition = null;
    for (int i = startPos; i < endPos; i++) {
      LOG.info("Write partition " + partIds.get(i) + " of matrix " + matrix.getName() + " to "
        + tmpDestFile);
      streamPos = out.getPos();
      partition = matrix.getPartition(partIds.get(i));
      PartitionKey partKey = partition.getPartitionKey();
      MatrixPartitionMeta partMeta =
        new MatrixPartitionMeta(partKey.getPartitionId(), partKey.getStartRow(),
          partKey.getEndRow(), partKey.getStartCol(), partKey.getEndCol(), partition.elementNum(),
          destFile.getName(), streamPos, 0);
      save(partition, partMeta, saveContext, out);
      partMeta.setLength(out.getPos() - streamPos);
      dataFilesMeta.addPartitionMeta(partIds.get(i), partMeta);
    }
    out.flush();
    out.close();
    HdfsUtil.rename(tmpDestFile, destFile, fs);
  }

  private void load(ServerMatrix matrix, Path matrixPath, FileSystem fs, List<Integer> partIds,
    PSMatrixLoadContext loadContext, int startPos, int endPos, PSMatrixFilesMeta dataFilesMeta)
    throws IOException {

    ServerPartition partition = null;
    FSDataInputStream input = null;
    long offset = 0;
    String currentFileName = "";
    for (int i = startPos; i < endPos; i++) {
      partition = matrix.getPartition(partIds.get(i));
      MatrixPartitionMeta partMeta = dataFilesMeta.getPartitionMeta(partIds.get(i));
      String fileName = partMeta.getFileName();
      offset = partMeta.getOffset();
      if (!fileName.equals(currentFileName)) {
        currentFileName = fileName;
        if (input != null) {
          input.close();
        }
        input = fs.open(new Path(matrixPath, currentFileName));
      }
      input.seek(offset);
      load(partition, partMeta, loadContext, input);
    }

    if (input != null) {
      input.close();
    }
  }

  protected List<Integer> filter(ServerPartition part, List<Integer> rowIds) {
    List<Integer> ret = new ArrayList<>();
    PartitionSource rows = part.getRows();

    for (int rowId : rowIds) {
      if (rows.hasRow(rowId)) {
        ret.add(rowId);
      }
    }

    return ret;
  }
}
