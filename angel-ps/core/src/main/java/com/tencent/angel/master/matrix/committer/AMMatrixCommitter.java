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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.*;
import com.tencent.angel.master.ps.ps.AMParameterServer;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.utils.HdfsUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
  
  /**commit tasks pool*/
  private ExecutorService committerPool;
  
  /**Is stop the dispatcher and commit tasks*/
  private final AtomicBoolean stopped;
  
  /**commit task list*/
  private final List<CommitTask> committers;
  
  /**max wait time*/
  private int waitTimeMS;
  
  private FileSystem fs;
  private static String resultDir = "result";

  /**
   * Create a AMMatrixCommitter
   * @param context master context
   * @param outputPath the final output directory
   * @param tmpOutputPath temporary output directory
   */
  public AMMatrixCommitter(AMContext context, Path outputPath, Path tmpOutputPath) {
    super(AMMatrixCommitter.class.getName());
    this.context = context;
    this.outputPath = outputPath;
    this.tmpOutputPath = tmpOutputPath;
    this.stopped = new AtomicBoolean(false);
    this.committers = new ArrayList<CommitTask>();
  }

  @Override
  protected void serviceStart() throws Exception {
    startCommitDispacherThread();
    super.serviceStart();
  }

  private void startCommitDispacherThread() {
    commitDispatchThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Map<ParameterServerId, AMParameterServer> psMap =
            context.getParameterServerManager().getParameterServerMap();

        try {
          for (Entry<ParameterServerId, AMParameterServer> entry : psMap.entrySet()) {
            CommitTask task = new CommitTask(entry.getValue());
            committers.add(task);
            committerPool.execute(task);
          }

          committerPool.shutdown();
          boolean ret = committerPool.awaitTermination(waitTimeMS, TimeUnit.MILLISECONDS);
          if (ret) {
            int size = committers.size();
            for (int i = 0; i < size; i++) {
              if (!committers.get(i).isSuccess()) {
                commitFailed(committers.get(i).getErrorLog());
                return;
              }
            }
            finalCommit();
            commitSuccess();
          } else {
            commitFailed("commit timeout, time setting is " + waitTimeMS);
          }
        } catch (Exception x) {
          LOG.error("commit error", x);
          commitFailed("commit error, " + x.getMessage());
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
    int committerNum =
        conf.getInt(AngelConf.ANGEL_AM_COMMIT_TASK_NUM,
            AngelConf.DEFAULT_ANGEL_AM_COMMIT_TASK_NUM);
    waitTimeMS =
        conf.getInt(AngelConf.ANGEL_AM_COMMIT_TIMEOUT_MS,
            AngelConf.DEFAULT_ANGEL_AM_COMMIT_TIMEOUT_MS);
    ThreadFactory commitThreadFacotry =
        new ThreadFactoryBuilder().setNameFormat("CommitTask").build();
    if (committerNum < 0 || committerNum > context.getParameterServerManager().getPsNumber()) {
      committerPool = Executors.newCachedThreadPool(commitThreadFacotry);
    } else {
      committerPool = Executors.newFixedThreadPool(committerNum, commitThreadFacotry);
    }

    fs = outputPath.getFileSystem(conf);
    tmpCombinePath = HdfsUtil.toFinalPath(tmpOutputPath);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      return;
    }

    committerPool.shutdownNow();

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

  private class CommitTask implements Runnable {

    private final AMParameterServer ps;
    private boolean success = false;
    private String errorLog;

    public CommitTask(AMParameterServer ps) {
      this.ps = ps;
    }

    @Override
    public void run() {
      try {
        Path psPath = new Path(new Path(tmpOutputPath, resultDir), String.valueOf(ps.getId()));
        HdfsUtil.copyFilesInSameHdfs(psPath, tmpCombinePath, fs);
        success = true;
        LOG.info("copy files from " + psPath + " to " + tmpCombinePath + " success ");
      } catch (Exception x) {
        errorLog =
            "copy files from " + tmpOutputPath + "/" + String.valueOf(ps.getId())
                + " failed, error log is " + x.getMessage();
        LOG.error(errorLog);
      }
    }

    public boolean isSuccess() {
      return success;
    }

    public String getErrorLog() {
      return errorLog;
    }
  }
}
