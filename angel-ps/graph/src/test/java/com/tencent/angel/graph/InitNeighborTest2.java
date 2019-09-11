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
package com.tencent.angel.graph;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.client.initneighbor2.InitNeighbor;
import com.tencent.angel.graph.client.initneighbor2.InitNeighborParam;
import com.tencent.angel.graph.client.sampleneighbor2.SampleNeighbor;
import com.tencent.angel.graph.client.sampleneighbor2.SampleNeighborParam;
import com.tencent.angel.graph.client.sampleneighbor2.SampleNeighborResult;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSErrorRequest;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServer;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InitNeighborTest2 {

  public static String DENSE_DOUBLE_MAT = "dense_double_mat";
  public static String DENSE_DOUBLE_MAT_COMP = "dense_double_mat_comp";
  public static String SPARSE_DOUBLE_MAT = "sparse_double_mat";
  public static String SPARSE_DOUBLE_MAT_COMP = "sparse_double_mat_comp";

  public static String DENSE_FLOAT_MAT = "dense_float_mat";
  public static String DENSE_FLOAT_MAT_COMP = "dense_float_mat_comp";
  public static String SPARSE_FLOAT_MAT = "sparse_float_mat";
  public static String SPARSE_FLOAT_MAT_COMP = "sparse_float_mat_comp";

  public static String DENSE_INT_MAT = "dense_int_mat";
  public static String DENSE_INT_MAT_COMP = "dense_int_mat_comp";
  public static String SPARSE_INT_MAT = "sparse_int_mat";
  public static String SPARSE_INT_MAT_COMP = "sparse_int_mat_comp";

  public static String DENSE_LONG_MAT = "dense_long_mat";
  public static String DENSE_LONG_MAT_COMP = "dense_long_mat_comp";
  public static String SPARSE_LONG_MAT = "sparse_long_mat";
  public static String SPARSE_LONG_MAT_COMP = "sparse_long_mat_comp";

  public static String DENSE_DOUBLE_LONG_MAT_COMP = "dense_double_long_mat_comp";
  public static String SPARSE_DOUBLE_LONG_MAT = "sparse_double_long_mat";
  public static String SPARSE_DOUBLE_LONG_MAT_COMP = "sparse_double_long_mat_comp";

  public static String DENSE_FLOAT_LONG_MAT_COMP = "dense_float_long_mat_comp";
  public static String SPARSE_FLOAT_LONG_MAT = "sparse_float_long_mat";
  public static String SPARSE_FLOAT_LONG_MAT_COMP = "sparse_float_long_mat_comp";

  public static String DENSE_INT_LONG_MAT_COMP = "dense_int_long_mat_comp";
  public static String SPARSE_INT_LONG_MAT = "sparse_int_long_mat";
  public static String SPARSE_INT_LONG_MAT_COMP = "sparse_int_long_mat_comp";

  public static String DENSE_LONG_LONG_MAT_COMP = "dense_long_long_mat_comp";
  public static String SPARSE_LONG_LONG_MAT = "sparse_long_long_mat";
  public static String SPARSE_LONG_LONG_MAT_COMP = "sparse_long_long_mat_comp";

  private static final Log LOG = LogFactory.getLog(InitNeighborTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 100000;
  int start = 100;
  int end = 1000000;
  int nnz = 1000;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setup() throws Exception {
    // set basic configuration keys
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, "file:///F:\\test\\model_1");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    //conf.setInt(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_SIZE, 1000);

    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 10);
    conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1);
    conf.setInt(AngelConf.ANGEL_PS_MAX_ATTEMPTS, 3);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add sparse float matrix
    MatrixContext siMat = new MatrixContext();
    siMat.setName(SPARSE_INT_MAT);
    siMat.setRowType(RowType.T_ANY_LONGKEY_SPARSE);
    siMat.setRowNum(1);
    siMat.setValidIndexNum(100);
    siMat.setColNum(10000000000L);
    siMat.setValueType(LongArrayElement.class);
    //siMat.setPartitionStorageClass(LongElementMapStorage.class);
    //siMat.setPartitionClass(CSRPartition.class);
    angelClient.addMatrix(siMat);

    // Start PS
    angelClient.startPSServer();

    // Start to run application
    angelClient.run();

    Thread.sleep(5000);

    psId = new ParameterServerId(0);
    psAttempt0Id = new PSAttemptId(psId, 0);

    WorkerGroupId workerGroupId = new WorkerGroupId(0);
    workerId = new WorkerId(workerGroupId, 0);
    workerAttempt0Id = new WorkerAttemptId(workerId, 0);
  }

  @Test
  public void testCSR() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client = worker.getPSAgent().getMatrixClient(SPARSE_INT_MAT, 0);
    int matrixId = client.getMatrixId();

    ParameterServer ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
    Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
            .getLocation();
    TConnection connection = TConnectionManager.getConnection(ps.getConf());
    MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());

    // Init node neighbors
    Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>();

    nodeIdToNeighbors.put(1, new long[]{2, 3, 4, 5, 6});
    nodeIdToNeighbors.put(2, new long[]{4, 5});
    nodeIdToNeighbors.put(3, new long[]{4, 5, 6});
    nodeIdToNeighbors.put(4, new long[]{5, 6});
    nodeIdToNeighbors.put(5, new long[]{6});
    nodeIdToNeighbors.put(8, new long[]{3, 4});
    InitNeighbor func = new InitNeighbor(new InitNeighborParam(matrixId, nodeIdToNeighbors));
    client.asyncUpdate(func).get();
    nodeIdToNeighbors.clear();

    /*nodeIdToNeighbors.put(1, new long[]{4, 5, 6});
    nodeIdToNeighbors.put(2, new long[]{5});
    nodeIdToNeighbors.put(4, new long[]{5, 6});
    func = new InitNeighbor(new InitNeighborParam(matrixId, nodeIdToNeighbors));
    client.asyncUpdate(func).get();
    nodeIdToNeighbors.clear();

    nodeIdToNeighbors.put(3, new long[]{4, 5, 6});
    nodeIdToNeighbors.put(5, new long[]{6});
    nodeIdToNeighbors.put(8, new long[]{3, 4});
    func = new InitNeighbor(new InitNeighborParam(matrixId, nodeIdToNeighbors));
    client.asyncUpdate(func).get();
    nodeIdToNeighbors.clear();
    */

    //client.asyncUpdate(new InitNeighborOver(new InitNeighborOverParam(matrixId))).get();

    // Sample the neighbors
    long[] nodeIds = new long[]{1, 2, 3, 4, 5, 6, 7, 8};
    SampleNeighborParam param = new SampleNeighborParam(matrixId, nodeIds, 2);
    Long2ObjectOpenHashMap<long[]> result = ((SampleNeighborResult) (client
        .get(new SampleNeighbor(param)))).getNodeIdToNeighbors();
    ObjectIterator<Long2ObjectMap.Entry<long[]>> iter = result
        .long2ObjectEntrySet().fastIterator();

    LOG.info("==============================sample neighbors result============================");
    Long2ObjectMap.Entry<long[]> entry;
    while (iter.hasNext()) {
      entry = iter.next();
      LOG.info(
          "node id = " + entry.getLongKey() + ", neighbors = " + Arrays.toString(entry.getValue()));
    }

    client.checkpoint(0).get();

    ps.stop(-1);
    PSErrorRequest request = PSErrorRequest.newBuilder()
        .setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt0Id))
        .setMsg("out of memory").build();
    master.psError(null, request);

    Thread.sleep(10000);

    param = new SampleNeighborParam(matrixId, nodeIds, -1);
    result = ((SampleNeighborResult) (client
        .get(new SampleNeighbor(param)))).getNodeIdToNeighbors();
    iter = result
        .long2ObjectEntrySet().fastIterator();

    LOG.info("==============================sample neighbors result============================");
    while (iter.hasNext()) {
      entry = iter.next();
      LOG.info(
          "node id = " + entry.getLongKey() + ", neighbors = " + Arrays.toString(entry.getValue()));
    }
  }

  @After
  public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}
