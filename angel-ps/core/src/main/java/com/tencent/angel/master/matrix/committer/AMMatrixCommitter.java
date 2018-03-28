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

package com.tencent.angel.master.matrix.committer;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.AppEvent;
import com.tencent.angel.master.app.AppEventType;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.model.io.IOExecutors;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.PSModelFilesMeta;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.utils.HdfsUtil;
import com.tencent.angel.model.output.format.ModelFilesMeta;

import com.tencent.angel.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Matrices committer in Master. After all parameter servers write the partitions of matrices to files in
 * temporary output directory, it rename the temporary output directory to the final output
 * directory.
 *
 */
public class AMMatrixCommitter extends AbstractService {
  private static final Log LOG = LogFactory.getLog(AMMatrixCommitter.class);
  
  /**master context*/
  private final AMContext context;
  
  /**final output path*/
  private final Path outputPath;
  
  /**temporary output path*/
  private final Path tmpOutputPath;
  
  /**temporary combine path*/
  private Path tmpCombinePath;

  /**the dispatcher of commit tasks*/
  private Thread commitDispatchThread;
  
  /**Is stop the dispatcher and commit tasks*/
  private final AtomicBoolean stopped;

  /** HDFS operation executor */
  private volatile IOExecutors fileOpExecutor;
  
  private FileSystem fs;

  /**
   * Need save matrix ids
   */
  private final List<Integer> needSaveMatrixIds;

  /**
   * Create a AMMatrixCommitter
   * @param context master context
   * @param outputPath the final output directory
   * @param tmpOutputPath temporary output directory
   */
  public AMMatrixCommitter(AMContext context, Path outputPath, Path tmpOutputPath, List<Integer> needSaveMatrixIds) {
    super(AMMatrixCommitter.class.getName());
    this.context = context;
    this.outputPath = outputPath;
    this.tmpOutputPath = tmpOutputPath;
    this.stopped = new AtomicBoolean(false);
    this.needSaveMatrixIds = needSaveMatrixIds;
  }

  /**
   * Matrices commit operator
   */
  class MatrixCommitOp extends RecursiveAction {
    private final List<Integer> matrixIds;
    private final Vector<String> errorLogs;
    private final int startPos;
    private final int endPos;

    public MatrixCommitOp(List<Integer> matrixIds, Vector<String> errorLogs, int startPos, int endPos) {
      this.matrixIds = matrixIds;
      this.errorLogs = errorLogs;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        try {
          commitMatrix(matrixIds.get(startPos), errorLogs);
        } catch (Throwable e) {
          String matrixName = context.getMatrixMetaManager().getMatrix(matrixIds.get(startPos)).getName();
          errorLogs.add("merge output files for matrix " + matrixName + " failed, error log is " + e.getMessage());
          LOG.error("merge output files for matrix " + matrixName + " failed. ", e);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        MatrixCommitOp
          opLeft = new MatrixCommitOp(matrixIds, errorLogs, startPos, middle);
        MatrixCommitOp
          opRight = new MatrixCommitOp(matrixIds, errorLogs, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }

  /**
   * Combine all output files of a model to a combine directory
   * @param matrixId matrix id
   * @param errorLogs error logs
   */
  private void commitMatrix(int matrixId, Vector<String> errorLogs) {
    LOG.info("start commit matrix " + matrixId);

    // Init matrix files meta
    List<ParameterServerId> psIds = new ArrayList<>(context.getMatrixMetaManager().getMasterPsIds(matrixId));
    MatrixMeta meta = context.getMatrixMetaManager().getMatrix(matrixId);
    Map<String, String> kvMap = meta.getAttributes();

    ModelFilesMeta filesMeta = new ModelFilesMeta(matrixId, meta.getName(), meta.getRowType().getNumber(),
      meta.getRowNum(), meta.getColNum(), meta.getBlockRowNum(), meta.getBlockColNum(), kvMap);

    try {
      // Move output files
      Path srcPath = new Path(tmpOutputPath, ModelFilesConstent.resultDirName);
      Path destPath = new Path(tmpCombinePath, meta.getName());
      PartitionCommitOp partCommitOp = new PartitionCommitOp(srcPath, destPath, psIds, errorLogs, filesMeta, 0, psIds.size());
      fileOpExecutor.execute(partCommitOp);
      partCommitOp.join();

      // Write the meta file
      long startTs = System.currentTimeMillis();
      Path metaFile = new Path(destPath, ModelFilesConstent.modelMetaFileName);
      Path tmpMetaFile = HdfsUtil.toTmpPath(metaFile);
      FSDataOutputStream metaOut = fs.create(tmpMetaFile);
      filesMeta.write(metaOut);
      metaOut.flush();
      metaOut.close();
      HdfsUtil.rename(tmpMetaFile, metaFile, fs);
      LOG.info("commit meta file use time=" + (System.currentTimeMillis() - startTs));
    } catch (Throwable x) {
      errorLogs.add("move output files for matrix " + meta.getName() + " failed, error msg = " + x.getMessage());
      LOG.error("move output files for matrix " + meta.getName() + " failed.", x);
    }
  }

  /**
   * Model partitions committer
   */
  class PartitionCommitOp extends RecursiveAction {
    private final Path moveSrcPath;
    private final Path moveDestPath;
    private final List<ParameterServerId> psList;
    private final Vector<String> errorLogs;
    private final ModelFilesMeta matrixMeta;
    private final int startPos;
    private final int endPos;

    public PartitionCommitOp(Path moveSrcPath, Path moveDestPath, List<ParameterServerId> psList,
      Vector<String> errorLogs, ModelFilesMeta matrixMeta, int startPos, int endPos) {
      this.moveSrcPath = moveSrcPath;
      this.moveDestPath = moveDestPath;
      this.psList = psList;
      this.errorLogs = errorLogs;
      this.matrixMeta = matrixMeta;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        commitPartitions(moveSrcPath, moveDestPath, psList.get(startPos), errorLogs, matrixMeta);
      } else {
        int middle = (startPos + endPos) / 2;
        PartitionCommitOp
          opLeft = new PartitionCommitOp(moveSrcPath, moveDestPath, psList, errorLogs, matrixMeta,  startPos, middle);
        PartitionCommitOp
          opRight = new PartitionCommitOp(moveSrcPath, moveDestPath, psList, errorLogs, matrixMeta, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }


  /**
   *  Move all model output files generated by a PS to the combine directory
   * @param moveSrcPath source path
   * @param moveDestPath dest path
   * @param psId parameter server id
   * @param errorLogs error logs
   * @param matrixMeta model files meta
   */
  private void commitPartitions(Path moveSrcPath, Path moveDestPath, ParameterServerId psId, Vector<String> errorLogs, ModelFilesMeta matrixMeta) {
    Path psPath = new Path(moveSrcPath, String.valueOf(psId));
    Path serverMatrixPath = new Path(psPath, matrixMeta.getMatrixName());

    Path psMetaFilePath = new Path(serverMatrixPath, ModelFilesConstent.psModelMetaFileName);

    try {
      FSDataInputStream input = fs.open(psMetaFilePath);
      PSModelFilesMeta serverMatrixMeta = new PSModelFilesMeta();
      serverMatrixMeta.read(input);
      input.close();
      fs.delete(psMetaFilePath, false);

      matrixMeta.merge(serverMatrixMeta);
      HdfsUtil.copyFilesInSameHdfs(serverMatrixPath, moveDestPath, fs);
      LOG.info("copy files of matrix " + matrixMeta.getMatrixName() + " from " + serverMatrixPath + " to " + moveDestPath + " success.");
    } catch (Throwable x) {
      errorLogs.add("copy files of matrix " + matrixMeta.getMatrixName() + " from " + serverMatrixPath + " to " + moveDestPath + " failed, error log is " + x.getMessage());
      LOG.error("copy files of matrix " + matrixMeta.getMatrixName() + " from " + serverMatrixPath + " to " + moveDestPath + " failed. ", x);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    startCommitDispacherThread();
    super.serviceStart();
  }

  private void startCommitDispacherThread() {
    if(needSaveMatrixIds == null || needSaveMatrixIds.isEmpty()) {
      LOG.info("there are no matrices need save");
      return;
    }

    commitDispatchThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Vector<String> errorLogs = new Vector<>();
        MatrixCommitOp op = new MatrixCommitOp(needSaveMatrixIds, errorLogs, 0, needSaveMatrixIds.size());
        fileOpExecutor = new IOExecutors(context.getConf().getInt(AngelConf.ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE,
          AngelConf.DEFAULT_ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE));
        fileOpExecutor.init();
        fileOpExecutor.start();

        try {
          fileOpExecutor.execute(op);
          op.join();
          if(!errorLogs.isEmpty()) {
            String errorLog = "move output files for matrice failed, error msg = " + StringUtils.join(";", errorLogs);
            LOG.error(errorLog);
            commitFailed(errorLog);
          } else {
            finalCommit();
            commitSuccess();
          }
        } catch (Throwable x) {
          LOG.error("move output files for matrice failed. ", x);
          commitFailed("move output files for matrice failed, error msg = " + x.getMessage());
        }
      }
    });

    commitDispatchThread.setName("CommitTaskDispacher");
    commitDispatchThread.start();
  }

  private void finalCommit() throws IOException {
    HdfsUtil.rename(tmpCombinePath, outputPath, fs);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    fs = outputPath.getFileSystem(conf);
    tmpCombinePath = HdfsUtil.toFinalPath(tmpOutputPath);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      return;
    }

    if(fileOpExecutor != null) {
      fileOpExecutor.shutdown();
    }

    if (commitDispatchThread != null) {
      commitDispatchThread.interrupt();
      try {
        commitDispatchThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("InterruptedException while stopping", ie);
      }
    }
    super.serviceStop();
  }

  @SuppressWarnings("unchecked")
  protected void commitSuccess() {
    context.getEventHandler()
        .handle(new AppEvent(context.getApplicationId(), AppEventType.SUCCESS));
  }

  @SuppressWarnings("unchecked")
  private void commitFailed(String errorLog) {
    context.getEventHandler().handle(new InternalErrorEvent(context.getApplicationId(), errorLog));
  }
}
