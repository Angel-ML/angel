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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.master;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.GetAllPSLocationRequest;
import com.tencent.angel.protobuf.generated.MLProtos.GetAllPSLocationResponse;
import com.tencent.angel.protobuf.generated.MLProtos.LocationProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSLocation;
import com.tencent.angel.protobuf.generated.MLProtos.PSStatus;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetPSLocationReponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetPSLocationRequest;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.ParameterServer;

public class LocationManagerTest {
  private static final Log LOG = LogFactory.getLog(LocationManagerTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setup() throws Exception {
    // set basic configuration keys
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConfiguration.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
    conf.set(AngelConfiguration.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 2);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add matrix
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w1");
    mMatrix.setRowNum(1);
    mMatrix.setColNum(100000);
    mMatrix.setMaxRowNumInBlock(1);
    mMatrix.setMaxColNumInBlock(50000);
    mMatrix.setRowType(MLProtos.RowType.T_INT_DENSE);
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_INT");
    angelClient.addMatrix(mMatrix);

    angelClient.startPSServer();
    angelClient.run();
    Thread.sleep(5000);
    psId = new ParameterServerId(0);
    psAttempt0Id = new PSAttemptId(psId, 0);
  }

  @Test
  public void testLocationManager() throws ServiceException, IOException {
    AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
    ParameterServer ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();

    String host = ps.getPsService().getHostAddress();
    int port = ps.getPsService().getPort();
    LocationManager locManager = angelAppMaster.getAppContext().getLocationManager();
    LocationProto psLoc = locManager.getPSLocation(psId);
    assertEquals(host, psLoc.getIp());
    assertEquals(port, psLoc.getPort());

    List<PSLocation> psLocs = locManager.getAllPSLocation();
    assertEquals(psLocs.size(), 1);
    assertEquals(psLocs.get(0).getLocation().getIp(), psLoc.getIp());
    assertEquals(psLocs.get(0).getLocation().getPort(), psLoc.getPort());

    Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
            .getLocation();
    TConnection connection = TConnectionManager.getConnection(ps.getConf());
    MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());
    PSAttemptId ps2Attempt0Id = new PSAttemptId(new ParameterServerId(2), 0);
    GetPSLocationRequest request1 =
        GetPSLocationRequest.newBuilder()
            .setPsId(ProtobufUtil.convertToIdProto(ps2Attempt0Id.getParameterServerId())).build();
    GetPSLocationReponse response1 = master.getPSLocation(null, request1);
    assertEquals(response1.getPsStatus(), PSStatus.PS_NOTREADY);

    request1 =
        GetPSLocationRequest.newBuilder()
            .setPsId(ProtobufUtil.convertToIdProto(psAttempt0Id.getParameterServerId())).build();
    response1 = master.getPSLocation(null, request1);
    assertEquals(host, response1.getLocation().getIp());
    assertEquals(port, response1.getLocation().getPort());
    assertEquals(response1.getPsStatus(), PSStatus.PS_OK);

    GetAllPSLocationRequest request2 = GetAllPSLocationRequest.newBuilder().build();
    GetAllPSLocationResponse response2 = master.getAllPSLocation(null, request2);
    assertEquals(response2.getPsLocationsCount(), 1);
    assertEquals(host, response2.getPsLocations(0).getLocation().getIp());
    assertEquals(port, response2.getPsLocations(0).getLocation().getPort());
  }

  @After
  public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}
