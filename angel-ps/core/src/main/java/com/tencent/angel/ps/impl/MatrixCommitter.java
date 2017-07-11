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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.utils.HdfsUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Matrix committer,will asynchronously commit all partitions of matrices on parameter server
 */
public class MatrixCommitter {
  private static final Log LOG = LogFactory.getLog(MatrixCommitter.class);
  private final ParameterServer ps;
  private final Configuration conf;
  private ExecutorService commitTaskPool;
  private Path baseDir;
  private FileSystem fs;
  private static String resultDir = "result";
  private AtomicBoolean isCommitting = new AtomicBoolean(false);

  /**
   * The task commit matrix partition on according file.
   */
  public class CommitTask implements Runnable {

    private final ServerPartition partition;
    private final ServerMatrix matrix;
    private AtomicBoolean isSuccess = new AtomicBoolean(false);
    private AtomicBoolean finishFlag = new AtomicBoolean(false);
    private String errorLog;
    private final String matrixName;
    private final int matrixId;

    /**
     * Create a new Commit task.
     *
     * @param matrix     the matrix
     * @param partition  the partition
     * @param matrixName the matrix name
     * @param matrixId   the matrix id
     */
    public CommitTask(ServerMatrix matrix, ServerPartition partition, String matrixName,
        int matrixId) {
      this.matrix = matrix;
      this.partition = partition;
      this.matrixId = matrixId;
      if (matrixName == null || matrixName.isEmpty()) {
        this.matrixName = String.valueOf(matrixId);
      } else {
        this.matrixName = matrixName;
      }
    }

    @Override
    public void run() {
      long startTime = Time.monotonicNow();
      FSDataOutputStream out = null;
      try {
        assert matrixName != null;
        Path destMatrixPath = new Path(baseDir, matrixName);

        // mkdir does not throw exception if path exits
        fs.mkdirs(destMatrixPath);
        Path destFile =
            new Path(destMatrixPath, String.valueOf(partition.getPartitionKey().getPartitionId()));
        Path tmpDestFile = HdfsUtil.toTmpPath(destFile);

        out = fs.create(tmpDestFile, (short) 1);
        matrix.writeHeader(out);
        partition.commit(out);
        out.flush();
        out.close();
        out = null;

        fs.rename(tmpDestFile, destFile);
        isSuccess.set(true);
        finishFlag.set(true);
      } catch (Exception x) {
        errorLog =
            "commit partition " + partition.getPartitionKey().getMatrixId() + "/"
                + partition.getPartitionKey().getPartitionId() + " error";
        LOG.error(errorLog, x);
        isSuccess.set(false);
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (Exception e) {
            LOG.warn("Warning!", e);
          }
        }
        LOG.info("commit matrix " + matrix.getName() + " cost time: "
            + (Time.monotonicNow() - startTime) + "ms!");
        finishFlag.set(true);
      }
    }

    /**
     * Is success.
     *
     * @return true if success, else false
     */
    public boolean isSuccess() {
      return isSuccess.get();
    }

    /**
     * Gets error log
     *
     * @return the error log if exists
     */
    public String getErrorLog() {
      return errorLog;
    }

    /**
     * Gets matrix id for committing.
     *
     * @return the matrix id
     */
    public int getMatrixId() {
      return matrixId;
    }
  }

  /**
   * Create a new Matrix committer according to parameter server
   *
   * @param ps the ps
   */
  public MatrixCommitter(ParameterServer ps) {
    this.ps = ps;
    this.conf = ps.getConf();
  }

  /**
   * For committing Parameter Server's matrices.
   * @param matrixIds 
   */
  public void commit(final List<Integer> matrixIds) {
    if (isCommitting.get()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ps is commiting......");
      }
      return;
    }
    LOG.info("to start commit tasks!");
    isCommitting.set(true);
    Thread commitDispacher = new Thread(new Runnable() {
      @Override
      public void run() {
        long startTime = Time.monotonicNow();
        try {
          String outputPath = conf.get(AngelConfiguration.ANGEL_JOB_TMP_OUTPUT_PATH);
          LOG.info("outputPath=" + outputPath);
          if (outputPath == null) {
            throw new IOException("can not find output path setting");
          }

          baseDir = new Path(new Path(outputPath, resultDir), ps.getServerId().toString());
          fs = baseDir.getFileSystem(conf);
          if (fs.exists(baseDir)) {
            LOG.warn("ps temp output directory is already existed " + baseDir.toString());
            fs.delete(baseDir, true);
          }
          fs.mkdirs(baseDir);

          int commitThreadCount =
              conf.getInt(AngelConfiguration.ANGEL_PS_COMMIT_TASK_NUM,
                  AngelConfiguration.DEFAULT_ANGEL_PS_COMMIT_TASK_NUM);
          ThreadFactory commitThreadFacotry =
              new ThreadFactoryBuilder().setNameFormat("CommitTask").build();
          commitTaskPool = Executors.newFixedThreadPool(commitThreadCount, commitThreadFacotry);

          List<CommitTask> allCommitTasks = new ArrayList<CommitTask>();
          int size = matrixIds.size();
          for(int i = 0; i < size; i++) {
            ServerMatrix matrix = ps.getMatrixPartitionManager().getMatrix(matrixIds.get(i));
            if(matrix == null) {
              continue;
            }

            List<PartitionKey> partitionKeys = matrix.getTotalPartitionKeys();
            if(partitionKeys == null || partitionKeys.isEmpty()) {
              continue;
            }

            for (PartitionKey key : partitionKeys) {
              ServerPartition partition = matrix.getPartition(key);
              CommitTask task = new CommitTask(matrix, partition, matrix.getName(), matrix.getId());
              allCommitTasks.add(task);
              commitTaskPool.execute(task);
            }
          }

          boolean commitSuccess = true;
          String errorLog = null;
          for (CommitTask task : allCommitTasks) {
            while (!task.finishFlag.get()) {
              Thread.sleep(1000);
            }
            if (!task.isSuccess()) {
              commitSuccess = false;
              errorLog = task.getErrorLog();
            }
          }
          LOG.info("ps commit cost time: " + (Time.monotonicNow() - startTime) + "ms");
          // commitTaskPool.shutdown();
          commitTaskPool.shutdownNow();
          if (commitSuccess) {
            ps.done();
          } else {
            LOG.error("ps failed for " + errorLog);
            ps.failed(errorLog);
          }
        } catch (Exception x) {
          LOG.fatal("ps commit error ", x);
          ps.failed("commit failed." + x.getMessage());
        }
      }
    });

    commitDispacher.setName("commit dispacher");
    commitDispacher.start();
  }

  /**
   * Gets according parameter server.
   *
   * @return the parameter server
   */
  public ParameterServer getPs() {
    return ps;
  }

  /**
   * Gets conf.
   *
   * @return the conf
   */
  public Configuration getConf() {
    return conf;
  }
}
