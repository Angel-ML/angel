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

package com.tencent.angel.psagent.client;

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Location;
import com.tencent.angel.exception.TimeOutException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.MatrixPartitionRouter;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.utils.Time;
import com.tencent.angel.worker.WorkerContext;
import com.tencent.angel.worker.WorkerGroup;
import com.tencent.angel.worker.WorkerRef;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.RequestConverter;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.CheckMatricesCreatedRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.CheckMatricesCreatedResponse;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.*;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The RPC client to master use protobuf codec protocol
 */
public class MasterClient {
  private static final Log LOG = LogFactory.getLog(MasterClient.class);
  
  /**protobuf RPC client*/
  private MasterProtocol master;

  public MasterClient() {

  }
  
  /**
   * Init protobuf rpc client to master
   * 
   * @throws IOException connect to master failed
   */
  public void init() throws IOException{
    TConnection connection = TConnectionManager.getConnection(PSAgentContext.get().getConf());
    Location masterLoc = PSAgentContext.get().getPsAgent().getMasterLocation();
    this.master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());
  }

  /**
   * Get the ps location from master
   * 
   * @param psId ps id
   * @return Location ps location
   * @throws ServiceException rpc failed
   */
  public Location getPSLocation(ParameterServerId psId) throws ServiceException {
    GetPSLocationRequest request =
        GetPSLocationRequest.newBuilder().setPsId(ProtobufUtil.convertToIdProto(psId)).build();

    GetPSLocationReponse response;
    response = master.getPSLocation(null, request);
    if (response.getPsStatus() == PSStatus.PS_NOTREADY) {
      LOG.warn("location is not ready for " + psId);
      return null;
    }

    return new Location(response.getLocation().getIp(), response.getLocation().getPort());

  }

  /**
   * Get the locations of all parameter servers
   * 
   * @return Map<ParameterServerId, Location> ps id to location map
   * @throws ServiceException  rpc failed
   */
  public Map<ParameterServerId, Location> getPSLocations() throws ServiceException{
    GetAllPSLocationRequest request = GetAllPSLocationRequest.newBuilder().build();

    HashMap<ParameterServerId, Location> routingMap = new HashMap<>();
    try {
      GetAllPSLocationResponse response = master.getAllPSLocation(null, request);

      for (PSLocation location : response.getPsLocationsList()) {
        PSIdProto psIdProto = location.getPsId();
        if (PSStatus.PS_OK == location.getPsStatus()) {
          LocationProto locationProto = location.getLocation();
          routingMap.put(ProtobufUtil.convertToId(psIdProto), new Location(locationProto.getIp(),
              locationProto.getPort()));
        } else {
          routingMap.put(ProtobufUtil.convertToId(psIdProto), null);
        }
      }
    } catch (com.google.protobuf.ServiceException e) {
      LOG.error("get all ps locations from master failed.", e);
    }

    return routingMap;
  }


  /**
   * Get the meta data and partitions for all matrices, it will wait until the matrices are ready
   * 
   * @return GetAllMatrixInfoResponse the meta data and partitions for all matrices
   * @throws InterruptedException interrupted when sleep for next try
   * @throws ServiceException rpc failed
   */
  public GetAllMatrixInfoResponse getMatrices() throws InterruptedException, ServiceException {
    GetAllMatrixInfoRequest request = GetAllMatrixInfoRequest.newBuilder().build();
    while (true) {
      LOG.debug("to get matrixInfo from master.......");
      GetAllMatrixInfoResponse response = master.getAllMatrixInfo(null, request);

      if (response.getMatrixStatus() == MLProtos.MatrixStatus.M_OK) {
        return response;
      } else {
        if (response.getMatrixStatus() == MLProtos.MatrixStatus.M_NOT_READY) {
          Thread.sleep(PSAgentContext.get().getRequestSleepTimeMS());
        }
      }
    }
  }

  /**
   * PSAgent register to master
   * 
   * @return PSAgentRegisterResponse register response
   * @throws ServiceException rpc failed
   */
  public PSAgentRegisterResponse psAgentRegister() throws ServiceException {
    PSAgentRegisterRequest request =
        PSAgentRegisterRequest
            .newBuilder()
            .setPsAgentAttemptId(PSAgentContext.get().getIdProto())
            .setLocation(
                LocationProto.newBuilder().setIp(PSAgentContext.get().getIp()).setPort(10000)
                    .build()).build();

    return master.psAgentRegister(null, request);
  }

  /**
   * Report ps agent state to master
   * 
   * @return PSAgentReportResponse report response
   * @throws ServiceException rpc failed
   */
  public PSAgentReportResponse psAgentReport() throws ServiceException {
    PSAgentReportRequest request =
        PSAgentReportRequest.newBuilder().setPsAgentAttemptId(PSAgentContext.get().getIdProto())
            .build();

    return master.psAgentReport(null, request);
  }

  /**
   * Create a new matrix
   * 
   * @param matrixContext matrix configuration
   * @param timeOutMS maximun wait time in milliseconds
   * @return MatrixMeta matrix meta
   * @throws ServiceException rpc failed
   * @throws TimeOutException create matrix time out
   * @throws InterruptedException interrupted when wait
   * @throws IOException read matrix meta from hdfs failed
   */
  public MatrixMeta createMatrix(MatrixContext matrixContext, long timeOutMS)
      throws ServiceException, TimeOutException, InterruptedException, IOException {

    MatrixProto matrixProto = matrixContext.buildMatProto(PSAgentContext.get().getConf());

    CreateMatrixRequest createRequest =
        CreateMatrixRequest.newBuilder().setMatrixProto(matrixProto).build();
    
    MatrixMetaManager matrixManager = PSAgentContext.get().getMatrixMetaManager();

    CreateMatrixResponse createResponse = master.createMatrix(null, createRequest);
    LOG.debug("create matrix response = " + createResponse);
    if (createResponse.getMatrixStatus() == MatrixStatus.M_OK) {
      matrixManager.addMatrix(new MatrixMeta(matrixContext, createResponse.getMatrixId()));
      updateMatrixPartitionRouter(createResponse.getMatrixId(), matrixProto, PSAgentContext.get()
          .getMatrixPartitionRouter());
      return matrixManager.getMatrixMeta(createResponse.getMatrixId());
    } else {
      CheckMatricesCreatedRequest request = CheckMatricesCreatedRequest.newBuilder().addMatrixNames(matrixContext.getName()).build();
      CheckMatricesCreatedResponse response = null;
      while (true) {
        long startTs = Time.now();
        //if (matrixManager.getMatrixMeta(createResponse.getMatrixId()) != null) {
        //  return matrixManager.getMatrixMeta(createResponse.getMatrixId());
        //}

        response = master.checkMatricesCreated(null, request);
        if(response.getStatus(0) == MatrixStatus.M_OK) {
          LOG.debug("getMatrixInfo response is OK, add matrix to matrixManager now");
          matrixManager.addMatrix(new MatrixMeta(matrixContext, createResponse.getMatrixId()));
          updateMatrixPartitionRouter(createResponse.getMatrixId(), matrixProto, PSAgentContext
              .get().getMatrixPartitionRouter());
          return matrixManager.getMatrixMeta(createResponse.getMatrixId());
        } else {
          if (Time.now() - startTs > timeOutMS) {
            throw new TimeOutException("create matrix time out ", (Time.now() - startTs), timeOutMS);
          }

          Thread.sleep(1000);
        }
      }
    }
  }

  private void updateMatrixPartitionRouter(int matrixId, MatrixProto matrixProto,
      MatrixPartitionRouter router) {
    for (MLProtos.MatrixPartitionLocation location : matrixProto.getMatrixPartLocationList()) {
      PartitionKey partitionKey = ProtobufUtil.convertPartition(location.getPart());
      partitionKey.setMatrixId(matrixId);
      router.addPartition(partitionKey, ProtobufUtil.convertToId(location.getPsId()));
    }
  }

  /**
   * Release a matrix
   * 
   * @param matrix matrix meta
   * @throws ServiceException exception come from master
   * @throws InterruptedException interrupted when wait
   */
  public void releaseMatrix(MatrixMeta matrix) throws ServiceException {
    ReleaseMatrixRequest request =
        ReleaseMatrixRequest.newBuilder().setMatrixId(matrix.getId())
            .setMatrixName(matrix.getName()).build();
    master.releaseMatrix(null, request);
  }

  /**
   * Get worker group information:workers and data splits, it will wait until the worker group is ready
   * 
   * @return WorkerGroup worker group information
   * @throws ClassNotFoundException split class not found
   * @throws IOException deserialize data splits meta failed
   * @throws ServiceException rpc failed
   * @throws InterruptedException interrupted when wait for next try
   */
  public WorkerGroup getWorkerGroupMetaInfo() throws ClassNotFoundException, IOException,
      ServiceException, InterruptedException {
    GetWorkerGroupMetaInfoRequest request =
        GetWorkerGroupMetaInfoRequest.newBuilder()
            .setWorkerAttemptId(WorkerContext.get().getWorkerAttemptIdProto()).build();

    while (true) {
      GetWorkerGroupMetaInfoResponse response =
          master.getWorkerGroupMetaInfo(null, request);
      assert (response.getWorkerGroupStatus() != GetWorkerGroupMetaInfoResponse.WorkerGroupStatus.WORKERGROUP_EXITED);

      LOG.debug("GetWorkerGroupMetaInfoResponse response=" + response);

      if (response.getWorkerGroupStatus() == GetWorkerGroupMetaInfoResponse.WorkerGroupStatus.WORKERGROUP_OK) {
        // Deserialize data splits meta
        SplitClassification splits = null;
        if (response.getWorkerGroupMeta().getSplitsCount() > 0) {
          splits =
              ProtobufUtil.getSplitClassification(response.getWorkerGroupMeta().getSplitsList(),
                  WorkerContext.get().getConf());
        }

        // Get workers
        WorkerGroup group = new WorkerGroup(WorkerContext.get().getWorkerGroupId(), splits);
        for (WorkerMetaInfoProto worker : response.getWorkerGroupMeta().getWorkersList()) {
          WorkerRef workerRef =
              new WorkerRef(worker.getWorkerLocation().getWorkerAttemptId(), worker
                  .getWorkerLocation().getLocation(), worker.getTasksList());
          group.addWorkerRef(workerRef);
        }
        return group;
      } else {         
        Thread.sleep(WorkerContext.get().getRequestSleepTimeMS());
      }
    }
  }

  /**
   * Register to master, report the listening port
   * 
   * @return WorkerRegisterResponse worker register response
   * @throws ServiceException rpc falied
   */
  public WorkerRegisterResponse workerRegister() throws ServiceException {
    Location location = WorkerContext.get().getLocation();
    WorkerRegisterRequest request =
        WorkerRegisterRequest
            .newBuilder()
            .setWorkerAttemptId(WorkerContext.get().getWorkerAttemptIdProto())
            .setLocation(
                LocationProto.newBuilder().setIp(location.getIp())
                    .setPort(location.getPort()).build()).build();

    return master.workerRegister(null, request);
  }

  /**
   * Report worker state to master
   * 
   * @return WorkerReportResponse report response
   * @throws ServiceException rpc failed
   */
  public WorkerReportResponse workerReport() throws ServiceException {
    WorkerReportRequest request =
        RequestConverter.buildWorkerReportRequest(WorkerContext.get().getWorker());
    return master.workerReport(null, request);
  }

  /**
   * Notify ps agent failed message to master
   * 
   * @param msg ps agent detail failed message
   * @throws ServiceException rpc failed
   */
  public void psAgentError(String msg) throws ServiceException {
    PSAgentErrorRequest request =
        PSAgentErrorRequest.newBuilder()
            .setPsAgentAttemptId(PSAgentContext.get().getPsAgent().getIdProto()).setMsg(msg).build();
    master.psAgentError(null, request);
  }

  /**
   * Notify ps agent success message to master
   * 
   * @throws ServiceException rpc failed
   */
  public void psAgentDone() throws ServiceException {
    PSAgentDoneRequest request =
        PSAgentDoneRequest.newBuilder()
            .setPsAgentAttemptId(PSAgentContext.get().getPsAgent().getIdProto()).build();
    master.psAgentDone(null, request);
  }

  /**
   * Notify worker failed message to master
   * 
   * @param msg worker detail failed message
   * @throws ServiceException rpc failed
   */
  public void workerError(String msg) throws ServiceException {
    WorkerErrorRequest request =
        WorkerErrorRequest.newBuilder()
            .setWorkerAttemptId(WorkerContext.get().getWorkerAttemptIdProto()).setMsg(msg).build();
    master.workerError(null, request);
  }

  /**
   * Notify worker success message to master
   * 
   * @throws ServiceException rpc failed
   */
  public void workerDone() throws ServiceException {
    WorkerDoneRequest request =
        WorkerDoneRequest.newBuilder()
            .setWorkerAttemptId(WorkerContext.get().getWorkerAttemptIdProto()).build();
    master.workerDone(null, request);
  }

  /**
   * Task update clock value of a matrix 
   * 
   * @param taskIndex task index
   * @param matrixId matrix id
   * @param clock clock value
   * @throws ServiceException    参数
   */
  public void updateClock(int taskIndex, int matrixId, int clock) throws ServiceException {
    TaskClockRequest request =
        TaskClockRequest.newBuilder()
            .setTaskId(TaskIdProto.newBuilder().setTaskIndex(taskIndex).build())
            .setMatrixClock(MatrixClock.newBuilder().setMatrixId(matrixId).setClock(clock).build())
            .build();
    master.taskClock(null, request);
  }

  /**
   * Task update iteration number
   * 
   * @param taskIndex task index
   * @param iteration iteration number
   * @throws ServiceException rpc failed
   */
  public void taskIteration(int taskIndex, int iteration) throws ServiceException {
    TaskIterationRequest request =
        TaskIterationRequest.newBuilder()
            .setTaskId(TaskIdProto.newBuilder().setTaskIndex(taskIndex).build())
            .setIteration(iteration).build();
    master.taskIteration(null, request);
  }
}
