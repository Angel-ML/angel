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


package com.tencent.angel.ps.storage;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.model.PSMatricesLoadContext;
import com.tencent.angel.model.PSMatricesSaveContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.io.load.SnapshotRecover;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;

/**
 * The matrix partitions manager on the parameter server.
 */
public class MatrixStorageManager {
  private final static Log LOG = LogFactory.getLog(MatrixStorageManager.class);
  /**
   * matrixId->Matrix
   */
  private final ConcurrentHashMap<Integer, ServerMatrix> matrixIdToDataMap;

  private final PSContext context;

  private final SnapshotRecover recover;

  /**
   * Create a new Matrix partition manager.
   */
  public MatrixStorageManager(PSContext context) {
    this.context = context;
    matrixIdToDataMap = new ConcurrentHashMap<>();
    recover = new SnapshotRecover(context);
  }

  /**
   * Get matrix use matrix id
   *
   * @param matrixId matrix id
   * @return ServerMatrix matrix
   */
  public ServerMatrix getMatrix(int matrixId) {
    return matrixIdToDataMap.get(matrixId);
  }

  /**
   * Add a batch of matrices to parameter server
   *
   * @param matrixMetas matrices meta
   * @throws IOException
   */
  public void addMatrices(List<MatrixMeta> matrixMetas) throws IOException {
    int size = matrixMetas.size();
    for (int i = 0; i < size; i++) {
      addMatrix(matrixMetas.get(i));
    }
  }

  /**
   * Add a matrixto parameter server.
   *
   * @param matrixMeta the matrix partitions
   * @throws IOException load matrix partition from files failed
   */
  public void addMatrix(MatrixMeta matrixMeta) throws IOException {
    int matrixId = matrixMeta.getId();
    if (matrixIdToDataMap.containsKey(matrixId)) {
      LOG.warn("MatrixId [" + matrixId + "] has already been added.");
      return;
    }
    ServerMatrix serverMatrix = new ServerMatrix(matrixMeta, context);
    serverMatrix.init();
    matrixIdToDataMap.put(matrixId, serverMatrix);
    LOG.info("MatrixId [" + matrixId + "] added.");
  }


  /**
   * Remove matrices from parameter server.
   *
   * @param needReleaseMatrices the release matrices
   */
  public void removeMatrices(List<Integer> needReleaseMatrices) {
    int size = needReleaseMatrices.size();
    for (int i = 0; i < size; i++) {
      removeMatrix(needReleaseMatrices.get(i));
    }
  }

  /**
   * Remove matrix from parameter server
   *
   * @param matrixId
   */
  public void removeMatrix(int matrixId) {
    matrixIdToDataMap.remove(matrixId);
  }

  /**
   * Write matrices to output
   *
   * @param output the output
   * @throws IOException
   */
  public void writeSnapshot(DataOutputStream output) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("matrixMap size: " + matrixIdToDataMap.size());
    }
    output.writeInt(matrixIdToDataMap.size());
    for (Map.Entry<Integer, ServerMatrix> entry : matrixIdToDataMap.entrySet()) {
      output.writeInt(entry.getKey());
      entry.getValue().writeSnapshot(output);
    }
  }

  /**
   * Get a row split
   *
   * @param matrixId the matrix id
   * @param rowId    the row id
   * @param partId   the partition ids
   * @return the row if exists, else null
   */
  public ServerRow getRow(int matrixId, int rowId, int partId) {
    ServerMatrix matrix = matrixIdToDataMap.get(matrixId);
    if (matrix != null) {
      return matrix.getRow(partId, rowId);
    } else {
      return null;
    }
  }

  /**
   * Get a row split
   *
   * @param partKey partition key
   * @param rowId   row index
   * @return the row if exists, else null
   */
  public ServerRow getRow(PartitionKey partKey, int rowId) {
    return getRow(partKey.getMatrixId(), rowId, partKey.getPartitionId());
  }

  /**
   * Gets server partition of matrix by partition key
   *
   * @param partKey the partition key
   * @return the server partition if exists,else null
   */
  public ServerPartition getPart(PartitionKey partKey) {
    ServerMatrix matrix = matrixIdToDataMap.get(partKey.getMatrixId());
    if (matrix != null) {
      return matrix.getPartition(partKey.getPartitionId());
    } else {
      return null;
    }
  }

  /**
   * Gets server partition of matrix by partition key
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @return the server partition if exists,else null
   */
  public ServerPartition getPart(int matrixId, int partId) {
    ServerMatrix matrix = matrixIdToDataMap.get(matrixId);
    if (matrix != null) {
      return matrix.getPartition(partId);
    } else {
      return null;
    }
  }

  /**
   * Clear matrices on parameter server
   */
  public void clear() {
    matrixIdToDataMap.clear();
  }

  /**
   * Load matrices from files
   *
   * @param loadContext load context
   * @throws IOException
   */
  public void load(PSMatricesLoadContext loadContext) throws IOException {
    LOG.info("start to load matrices :" + loadContext);

    Vector<String> errorLogs = new Vector<>();
    try {
      MatrixDiskIOOp commitOp = new MatrixDiskIOOp(ACTION.LOAD, errorLogs, loadContext, 0,
        loadContext.getMatrixLoadContexts().size());
      context.getIOExecutors().execute(commitOp);
      commitOp.join();
      if (!errorLogs.isEmpty()) {
        throw new IOException(StringUtils.join("\n", errorLogs));
      }
    } catch (Throwable x) {
      throw new IOException(x);
    }
  }

  /**
   * Save matrices to files
   *
   * @param saveContext matrices save context
   * @throws IOException
   */
  public void save(PSMatricesSaveContext saveContext) throws IOException {
    LOG.info("start to save matrices :" + saveContext);
    Path baseDir = new Path(saveContext.getSavePath());
    FileSystem fs = baseDir.getFileSystem(context.getConf());
    if (fs.exists(baseDir)) {
      LOG.warn(
        "ps temp output directory " + baseDir.toString() + " is already existed , just remove it");
      fs.delete(baseDir, true);
    }

    Vector<String> errorLogs = new Vector<>();
    try {
      MatrixDiskIOOp commitOp = new MatrixDiskIOOp(ACTION.SAVE, errorLogs, saveContext, 0,
        saveContext.getMatrixSaveContexts().size());
      context.getIOExecutors().execute(commitOp);
      commitOp.join();
      if (!errorLogs.isEmpty()) {
        throw new IOException(StringUtils.join("\n", errorLogs));
      }
    } catch (Throwable x) {
      throw new IOException(x);
    }

    return;
  }

  private String getMatrixNames(Collection<Integer> matrixIds) {
    int size = matrixIds.size();
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (int matrixId : matrixIds) {
      ServerMatrix matrix = getMatrix(matrixId);
      if (matrix != null) {
        sb.append(matrix.getName());
      } else {
        sb.append("null");
      }

      if (i < size - 1) {
        sb.append(",");
      }
      i++;
    }

    return sb.toString();
  }

  public ConcurrentHashMap<Integer, ServerMatrix> getMatrices() {
    return matrixIdToDataMap;
  }

  enum ACTION {
    LOAD, SAVE
  }


  class MatrixDiskIOOp extends RecursiveAction {
    private final ACTION action;
    private final Vector<String> errorLogs;
    private final Object context;
    private final int startPos;
    private final int endPos;

    public MatrixDiskIOOp(ACTION action, Vector<String> errorLogs, Object context, int start,
      int end) {
      this.action = action;
      this.errorLogs = errorLogs;
      this.context = context;
      this.startPos = start;
      this.endPos = end;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        try {
          process(context, startPos, action);
        } catch (Throwable e) {
          LOG.error("load/save matrix failed ", e);
          errorLogs.add("load/save matrix failed:" + StringUtils.stringifyException(e));
        }
      } else {
        int middle = (startPos + endPos) / 2;
        MatrixDiskIOOp opLeft = new MatrixDiskIOOp(action, errorLogs, context, startPos, middle);
        MatrixDiskIOOp opRight = new MatrixDiskIOOp(action, errorLogs, context, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private void process(Object context, int index, ACTION action) throws IOException {
    switch (action) {
      case LOAD: {
        PSMatricesLoadContext loadContext = (PSMatricesLoadContext) context;
        if (loadContext.getLoadPath() != null) {
          loadMatrix(new Path(loadContext.getLoadPath()),
            loadContext.getMatrixLoadContexts().get(index));
        } else {
          loadMatrix(null, loadContext.getMatrixLoadContexts().get(index));
        }
        break;
      }

      case SAVE: {
        saveMatrix(new Path(((PSMatricesSaveContext) context).getSavePath()),
          ((PSMatricesSaveContext) context).getMatrixSaveContexts().get(index));
        break;
      }
    }
  }

  private void loadMatrix(Path loadPath, PSMatrixLoadContext loadContext) throws IOException {
    ServerMatrix matrix = context.getMatrixStorageManager().getMatrix(loadContext.getMatrixId());
    MatrixMeta matrixMeta = context.getMatrixMetaManager().getMatrixMeta(loadContext.getMatrixId());
    if (matrix != null) {
      if (loadPath != null) {
        LOG.info("Load matrix " + matrixMeta.getName() + " from path " + loadPath);
        matrix.load(matrixMeta, loadPath);
      } else if (loadContext.getLoadPath() != null) {
        matrix.load(matrixMeta, new Path(loadContext.getLoadPath()));
      } else {
        matrix.load(matrixMeta, null);
      }

      /*if(inputPath == null) {
        try {
          inputPath = recover.getSnapshotPath(matrixId);
        } catch (IOException e) {
          LOG.error("Get snapshot path failed, ", e);
        }

        if(inputPath == null) {
          String loadPathStr = matrixMeta.getAttribute(MatrixConf.MATRIX_LOAD_PATH);
          if(loadPathStr != null) {
            inputPath = new Path(loadPathStr);
          }
        }
      }*/
    }
  }

  private void saveMatrix(Path savePath, PSMatrixSaveContext saveContext) throws IOException {
    ServerMatrix matrix = matrixIdToDataMap.get(saveContext.getMatrixId());
    MatrixMeta matrixMeta = context.getMatrixMetaManager().getMatrixMeta(saveContext.getMatrixId());
    if (matrix != null) {
      matrix.save(matrixMeta, new Path(savePath, matrixMeta.getName()), saveContext);
    }
  }
}
