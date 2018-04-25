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

package com.tencent.angel.ps.impl;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ps.recovery.snapshot.SnapshotRecover;
import com.tencent.angel.utils.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;

/**
 * The matrix partitions manager on the parameter server.
 */
public class MatrixStorageManager {
  private final static Log LOG = LogFactory.getLog(MatrixStorageManager.class);
  /** matrixId->Matrix  */
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
   * @param matrixMetas matrices meta
   * @throws IOException
   */
  public void addMatrices(List<MatrixMeta> matrixMetas) throws IOException {
    int size = matrixMetas.size();
    for(int i = 0; i < size; i++) {
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
   * @param matrixId
   */
  public void removeMatrix(int matrixId) {
    matrixIdToDataMap.remove(matrixId);
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
   * @param partKey partition key
   * @param rowId row index
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
   * @param partId partition id
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
   * @param matrixIds need load matrix ids
   * @param baseDir files directory
   * @throws IOException
   */
  public void load(List<Integer> matrixIds, Path baseDir) throws IOException {
    if(matrixIds == null || matrixIds.isEmpty()) {
      LOG.info("there are no matrices load");
      return;
    }
    LOG.info("start to load matrices :" + getMatrixNames(matrixIds) + " to " + baseDir);

    Vector<String> errorLogs = new Vector<>();
    try {
      MatrixDiskIOOp commitOp = new MatrixDiskIOOp(baseDir, ACTION.LOAD, errorLogs, matrixIds,
        new HashMap<>(), 0, matrixIds.size(), false);
      context.getIOExecutors().execute(commitOp);
      commitOp.join();
      if(!errorLogs.isEmpty()) {
        throw new IOException(StringUtils.join("\n", errorLogs));
      }
    } catch (Throwable x) {
      throw new IOException(x);
    }
  }

  /**
   * Save matrices to files
   * @param matrixIds matrix ids
   * @param baseDir base save directory
   * @param cloneFirst clone the row first before saving
   * @throws IOException
   */
  public void save(List<Integer> matrixIds, Path baseDir, boolean cloneFirst) throws IOException {
    if(matrixIds == null || matrixIds.isEmpty()) {
      LOG.info("there are no matrices need save");
      return;
    }
    Map<Integer, List<Integer>> matrixPartitions = new HashMap<>(matrixIds.size());
    for(int matrixId : matrixIds) {
      matrixPartitions.put(matrixId, new ArrayList<>(
        context.getMatrixMetaManager().getMatrixMeta(matrixId).getPartitionMetas().keySet()));
    }

    save(matrixPartitions, baseDir, cloneFirst);
  }

  /**
   * Save matrices to files
   * @param matrixIds matrix ids
   * @param baseDir base save directory
   * @throws IOException
   */
  public void save(List<Integer> matrixIds, Path baseDir) throws IOException {
    save(matrixIds, baseDir, false);
  }

  /**
   * Save matrices to files
   * @param matrixPartitions matrix id -> need save partitions map
   * @param baseDir save directory
   * @param cloneFirst clone the row first before saving
   * @throws IOException
   */
  public void save(Map<Integer, List<Integer>> matrixPartitions, Path baseDir, boolean cloneFirst) throws IOException {
    if(matrixPartitions == null || matrixPartitions.isEmpty()) {
      LOG.info("there are no matrices need save");
      return;
    }
    LOG.info("start to write matrices :" + getMatrixNames(matrixPartitions.keySet()) + " to " + baseDir);
    FileSystem fs = baseDir.getFileSystem(context.getConf());
    if (fs.exists(baseDir)) {
      LOG.warn("ps temp output directory " + baseDir.toString() + " is already existed , just remove it");
      fs.delete(baseDir, true);
    }

    List<Integer> matrixIds = new ArrayList<>(matrixPartitions.keySet());
    Vector<String> errorLogs = new Vector<>();
    try {
      MatrixDiskIOOp commitOp = new MatrixDiskIOOp(baseDir, ACTION.SAVE, errorLogs, matrixIds, matrixPartitions, 0, matrixIds.size(), cloneFirst);
      context.getIOExecutors().execute(commitOp);
      commitOp.join();
      if(!errorLogs.isEmpty()) {
        throw new IOException(StringUtils.join("\n", errorLogs));
      }
    } catch (Throwable x) {
      throw new IOException(x);
    }

    return;
  }

  /**
   * Save matrices to files
   * @param matrixPartitions matrix id -> need save partitions map
   * @throws IOException
   */
  public void save(Map<Integer, List<Integer>> matrixPartitions, Path baseDir) throws IOException {
    save(matrixPartitions, baseDir, false);
  }

  private String getMatrixNames(Collection<Integer> matrixIds) {
    int size = matrixIds.size();
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for(int matrixId : matrixIds) {
      ServerMatrix matrix = getMatrix(matrixId);
      if(matrix != null) {
        sb.append(matrix.getName());
      } else {
        sb.append("null");
      }

      if(i < size - 1) {
        sb.append(",");
      }
      i++;
    }

    return sb.toString();
  }

  public ConcurrentHashMap<Integer,ServerMatrix> getMatrices() {
    return matrixIdToDataMap;
  }

  enum ACTION {
    LOAD, SAVE
  }

  class MatrixDiskIOOp extends RecursiveAction {
    private final Path path;
    private final ACTION action;
    private final Vector<String> errorLogs;
    private final List<Integer> matrixIds;
    private final Map<Integer, List<Integer>> matrixPartitions;
    private final int startPos;
    private final int endPos;
    private final boolean cloneFirst;

    public MatrixDiskIOOp(Path path, ACTION action, Vector<String> errorLogs,
      List<Integer> matrixIds, Map<Integer, List<Integer>> matrixPartitions, int start, int end, boolean cloneFirst) {
      this.path = path;
      this.action = action;
      this.errorLogs = errorLogs;
      this.matrixIds = matrixIds;
      this.matrixPartitions = matrixPartitions;
      this.startPos = start;
      this.endPos = end;
      this.cloneFirst = cloneFirst;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        try {
          process(path, matrixIds.get(startPos), matrixPartitions.get(matrixIds.get(startPos)), action, cloneFirst);
        } catch (Throwable e) {
          String errorLog = "commit matrix " + matrixIdToDataMap.get(matrixIds.get(startPos)).getName() + " failed " + e.getMessage();
          LOG.error(errorLog, e);
          errorLogs.add(errorLog);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        MatrixDiskIOOp opLeft = new MatrixDiskIOOp(path, action, errorLogs, matrixIds, matrixPartitions, startPos, middle, cloneFirst);
        MatrixDiskIOOp opRight = new MatrixDiskIOOp( path, action, errorLogs, matrixIds, matrixPartitions, middle, endPos, cloneFirst);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private void process(Path path, int matrixId, List<Integer> partIds, ACTION action, boolean cloneFirst)
    throws IOException {
    switch (action) {
      case LOAD:{
        loadMatrix(path, matrixId);
        break;
      }

      case SAVE:{
        saveMatrix(path, matrixId, partIds, cloneFirst);
        break;
      }
    }
  }

  private void loadMatrix(Path inputPath, int matrixId) throws IOException {
    ServerMatrix matrix = context.getMatrixStorageManager().getMatrix(matrixId);
    MatrixMeta matrixMeta = context.getMatrixMetaManager().getMatrixMeta(matrixId);
    if(matrix != null) {
      if(inputPath == null) {
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
      }
      matrix.load(matrixMeta, inputPath);
    }
  }

  private void saveMatrix(Path outputPath, int matrixId, List<Integer> partIds, boolean cloneFirst) throws IOException {
    ServerMatrix matrix = matrixIdToDataMap.get(matrixId);
    if(matrix != null) {
      matrix.save(context.getMatrixMetaManager().getMatrixMeta(matrixId), partIds, outputPath, cloneFirst);
    }
  }
}
