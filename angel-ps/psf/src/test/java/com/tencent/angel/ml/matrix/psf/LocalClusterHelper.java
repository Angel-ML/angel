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


package com.tencent.angel.ml.matrix.psf;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class LocalClusterHelper {
  protected static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  protected static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private static AngelClient angelClient;
  private static WorkerGroupId group0Id;
  private static WorkerId worker0Id;
  private static WorkerAttemptId worker0Attempt0Id;
  private static TaskId task0Id;
  private static TaskId task1Id;
  private static ParameterServerId psId;
  private static PSAttemptId psAttempt0Id;

  public static void setup() throws Exception {
    // set basic configuration keys
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

    // use local deploy mode and dummy data spliter
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 2);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add matrix
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w1");
    mMatrix.setRowNum(10);
    mMatrix.setColNum(1000);
    mMatrix.setMaxRowNumInBlock(10);
    mMatrix.setMaxColNumInBlock(500);
    mMatrix.setRowType(RowType.T_INT_DENSE);
    mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_INT");
    angelClient.addMatrix(mMatrix);

    MatrixContext mMatrix2 = new MatrixContext();
    mMatrix2.setName("w2");
    mMatrix2.setRowNum(10);
    mMatrix2.setColNum(100);
    mMatrix2.setMaxRowNumInBlock(5);
    mMatrix2.setMaxColNumInBlock(50);
    mMatrix2.setRowType(RowType.T_DOUBLE_DENSE);
    mMatrix2.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix2.set(MatrixConf.MATRIX_HOGWILD, "false");
    mMatrix2.set(MatrixConf.MATRIX_AVERAGE, "false");
    mMatrix2.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
    angelClient.addMatrix(mMatrix2);

    angelClient.startPSServer();
    angelClient.run();
    Thread.sleep(2 * 1000);
    group0Id = new WorkerGroupId(0);
    worker0Id = new WorkerId(group0Id, 0);
    worker0Attempt0Id = new WorkerAttemptId(worker0Id, 0);
    task0Id = new TaskId(0);
    task1Id = new TaskId(1);
    psId = new ParameterServerId(0);
    psAttempt0Id = new PSAttemptId(psId, 0);
  }

  public static void cleanup() throws Exception {
    angelClient.stop();
  }
}