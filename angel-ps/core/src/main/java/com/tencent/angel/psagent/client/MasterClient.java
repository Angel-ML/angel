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

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.exception.TimeOutException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.transport.PSLocation;
import com.tencent.angel.ml.matrix.transport.ServerState;
import com.tencent.angel.ml.metric.Metric;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.RequestConverter;
import com.tencent.angel.protobuf.generated.MLProtos.*;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.PSAgentPSServiceProtos.*;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.*;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.PSProtocol;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.utils.KryoUtils;
import com.tencent.angel.utils.Time;
import com.tencent.angel.worker.WorkerContext;
import com.tencent.angel.worker.WorkerGroup;
import com.tencent.angel.worker.WorkerRef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

/**
 * The RPC client to master use protobuf codec protocol
 */
public class MasterClient {
  private static final Log LOG = LogFactory.getLog(MasterClient.class);
  
  /**protobuf RPC client*/
  private volatile MasterProtocol master;

  public MasterClient() {

  }
  
  /**
   * Init protobuf rpc client to master
   * 
   * @throws IOException connect to master failed
   */
  public void init() throws IOException{
    this.master = getOrCreateMasterClient(PSAgentContext.get().getPsAgent().getMasterLocation());
  }

  private MasterProtocol getOrCreateMasterClient(Location loc) throws IOException {
    return PSAgentContext.get().getPsAgent().getControlConnectManager().getMasterService(loc.getIp(), loc.getPort());
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
    return ProtobufUtil.convertToLocation(master.getPSLocation(null, request).getPsLocation());
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
      List<PSLocationProto> psLocs = response.getPsLocationsList();
      int size = psLocs.size();
      for(int i = 0; i < size; i++) {
        routingMap.put(ProtobufUtil.convertToId(psLocs.get(i).getPsId()), ProtobufUtil.convertToLocation(psLocs.get(i)));
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
  public List<MatrixMeta> getMatrices()
    throws InterruptedException, ServiceException, ClassNotFoundException {
    GetAllMatrixMetaResponse response = master.getAllMatrixMeta(null, GetAllMatrixMetaRequest.newBuilder().build());
    List<MatrixMetaProto> matrixMetaProtos = response.getMatrixMetasList();
    int size = matrixMetaProtos.size();
    List<MatrixMeta> matrixMetas = new ArrayList<>(size);
    for(int i = 0; i< size; i++) {
      matrixMetas.add(ProtobufUtil.convertToMatrixMeta(matrixMetaProtos.get(i)));
    }

    return matrixMetas;
  }

  /**
   * Get a matrix meta
   * @param matrixName matrix name
   * @return matrix meta
   * @throws ServiceException
   * @throws ClassNotFoundException
   */
  public MatrixMeta getMatrix(String matrixName) throws ServiceException, ClassNotFoundException {
    GetMatricesResponse response = master.getMatrices(null, GetMatricesRequest.newBuilder().addMatrixNames(matrixName).build());
    return ProtobufUtil.convertToMatrixMeta(response.getMatrixMetas(0));
  }

  /**
   * Get matrix metas
   * @param matrixNames matrix names
   * @return matrix metas
   * @throws ServiceException
   * @throws ClassNotFoundException
   */
  public List<MatrixMeta> getMatrices(List<String> matrixNames)
    throws ServiceException, ClassNotFoundException {
    GetMatricesResponse response = master.getMatrices(null, GetMatricesRequest.newBuilder().addAllMatrixNames(matrixNames).build());
    List<MatrixMetaProto> matrixMetaProtos = response.getMatrixMetasList();
    int size = matrixMetaProtos.size();
    List<MatrixMeta> matrixMetas = new ArrayList<>(size);

    for(int i = 0; i < size; i++) {
      matrixMetas.add(ProtobufUtil.convertToMatrixMeta(matrixMetaProtos.get(i)));
    }
    return matrixMetas;
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
            .setPsAgentId(PSAgentContext.get().getPsAgent().getId())
            .setLocation(ProtobufUtil.convertToLocationProto(PSAgentContext.get().getLocation())).build();

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
        PSAgentReportRequest.newBuilder().setPsAgentId(PSAgentContext.get().getPsAgent().getId())
            .build();

    return master.psAgentReport(null, request);
  }

  /**
   * Create a new matrix
   * 
   * @param matrixContext matrix configuration
   * @param timeOutMS maximun wait time in milliseconds
   * @throws Exception rpc failed
   */
  public void createMatrix(MatrixContext matrixContext, long timeOutMS)
    throws Exception {
    matrixContext.init(PSAgentContext.get().getConf());
    List<MatrixContext> matrixContexts = new ArrayList<>(1);
    matrixContexts.add(matrixContext);
    createMatrices(matrixContexts, timeOutMS);
  }

  /**
   * Create a new matrix
   *
   * @param matrixContexts matrices configuration
   * @param timeOutMS maximun wait time in milliseconds
   * @throws Exception rpc failed
   */
  public void createMatrices(List<MatrixContext> matrixContexts, long timeOutMS)
    throws Exception {
    CreateMatricesRequest.Builder createBuilder = CreateMatricesRequest.newBuilder();
    CheckMatricesCreatedRequest.Builder checkBuilder = CheckMatricesCreatedRequest.newBuilder();
    List<String> matrixNames = new ArrayList<>(matrixContexts.size());

    int size = matrixContexts.size();
    for(int i = 0; i < size; i++) {
      matrixContexts.get(i).init(PSAgentContext.get().getConf());
      matrixNames.add(matrixContexts.get(i).getName());
      checkBuilder.addMatrixNames(matrixContexts.get(i).getName());
      createBuilder.addMatrices(ProtobufUtil.convertToMatrixContextProto(matrixContexts.get(i)));
    }

    LOG.info("start to create matrices " + String.join(",", matrixNames));
    master.createMatrices(null, createBuilder.build());

    CheckMatricesCreatedRequest checkRequest = checkBuilder.build();
    CheckMatricesCreatedResponse checkResponse = null;
    while (true) {
      long startTs = Time.now();
      checkResponse = master.checkMatricesCreated(null, checkRequest);
      if(checkResponse.getStatus() == 0) {
        LOG.info("create matrices " + String.join(",", matrixNames) + " success");

        List<MatrixMetaProto> metaProtos = master.getMatrices(null,
          GetMatricesRequest.newBuilder().addAllMatrixNames(matrixNames).build()).getMatrixMetasList();
        for(int i = 0; i < size; i++) {
          PSAgentContext.get().getMatrixMetaManager().addMatrix(ProtobufUtil.convertToMatrixMeta(metaProtos.get(i)));
        }
        return;
      } else {
        if (Time.now() - startTs > timeOutMS) {
          throw new TimeOutException("create matrix time out ", (Time.now() - startTs), timeOutMS);
        }
        Thread.sleep(1000);
      }
    }
  }

  /**
   * Release a matrix
   * 
   * @param matrixName matrix name
   * @throws ServiceException exception come from master
   * @throws InterruptedException interrupted when wait
   */
  public void releaseMatrix(String matrixName) throws ServiceException {
    List<String> matrixNames = new ArrayList<>(1);
    matrixNames.add(matrixName);
    releaseMatrices(matrixNames);
  }

  public void releaseMatrices(List<String> matrixNames) throws ServiceException {
    master.releaseMatrices(null, ReleaseMatricesRequest.newBuilder().addAllMatrixNames(matrixNames).build());
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
          .setPsAgentId(PSAgentContext.get().getPsAgent().getId()).setMsg(msg).build();
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
            .setPsAgentId(PSAgentContext.get().getPsAgent().getId()).build();
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
   * @throws ServiceException
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

  /**
   * Task update iteration number
   *
   * @param taskIndex task index
   * @param counters task counters
   * @throws ServiceException rpc failed
   */
  public void taskCountersUpdate(Map<String, String> counters, int taskIndex) throws ServiceException {
    TaskCounterUpdateRequest.Builder builder = TaskCounterUpdateRequest.newBuilder();
    builder.setTaskId(TaskIdProto.newBuilder().setTaskIndex(taskIndex).build());
    Pair.Builder kvBuilder = Pair.newBuilder();
    for(Map.Entry<String, String> kv:counters.entrySet()) {
      builder.addCounters(kvBuilder.setKey(kv.getKey()).setValue(kv.getValue()).build());
    }
    master.taskCountersUpdate(null, builder.build());
  }

  /**
   * Set Task algorithm metrics
   * @param taskIndex task index
   * @param algoMetrics algorithm metrics
   */
  public void setAlgoMetrics(int taskIndex, Map<String, Metric> algoMetrics)
    throws ServiceException {
    SetAlgoMetricsRequest.Builder builder = SetAlgoMetricsRequest.newBuilder();
    AlgoMetric.Builder metricBuilder = AlgoMetric.newBuilder();
    builder.setTaskId(TaskIdProto.newBuilder().setTaskIndex(taskIndex).build());
    for(Map.Entry<String, Metric> metricEntry:algoMetrics.entrySet()) {
      builder.addAlgoMetrics(metricBuilder.setName(metricEntry.getKey()).setSerializedMetric(
        ByteString.copyFrom(KryoUtils.serializeAlgoMetric(metricEntry.getValue()))).build());
    }
    master.setAlgoMetrics(null, builder.build());
  }

  /**
   * Get the pss that stored the partition
   * @param matrixId matrix id
   * @param partitionId partition id
   * @return the pss that stored the partition
   * @throws ServiceException
   */
  public List<ParameterServerId> getStoredPss(int matrixId, int partitionId)
    throws ServiceException {
    List<PSIdProto> psIdProtos = master.getStoredPss(null,
      GetStoredPssRequest.newBuilder().setMatrixId(matrixId).setPartId(partitionId).build()).getPsIdsList();
    int size = psIdProtos.size();
    List<ParameterServerId> psIds = new ArrayList<>(psIdProtos.size());
    for(int i = 0; i < size; i++) {
      psIds.add(ProtobufUtil.convertToId(psIdProtos.get(i)));
    }
    return psIds;
  }

  /**
   * Get the pss and their locations that stored the partition
   * @param matrixId matrix id
   * @param partId partition id
   * @return the pss and their locations that stored the partition
   * @throws ServiceException
   */
  public PartitionLocation getPartLocation(int matrixId, int partId) throws ServiceException {
    GetPartLocationResponse response = master.getPartLocation(null,
      GetPartLocationRequest.newBuilder().setMatrixId(matrixId).setPartId(partId).build());
    List<PSLocationProto> psLocsProto = response.getLocationsList();
    int size = psLocsProto.size();
    List<PSLocation> psLocs = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      psLocs.add(new PSLocation(ProtobufUtil.convertToId(psLocsProto.get(i).getPsId()), ProtobufUtil.convertToLocation(psLocsProto.get(i))));
    }
    return new PartitionLocation(psLocs);
  }

  /**
   * Report a ps failed to Master
   * @param psLoc ps id and location
   * @throws ServiceException
   */
  public void psFailed(PSLocation psLoc) throws ServiceException {
    master.psFailedReport(null, PSFailedReportRequest.newBuilder()
      .setClientId(PSAgentContext.get().getPSAgentId()).setPsLoc(ProtobufUtil.convert(psLoc)).build());
  }

  /**
   * Get the number of success worker group
   * @return the number of success worker group
   * @throws ServiceException
   */
  public int getSuccessWorkerGroupNum() throws ServiceException {
    return master.getWorkerGroupSuccessNum(null,
      GetWorkerGroupSuccessNumRequest.getDefaultInstance().newBuilder().build()).getSuccessNum();
  }

  /**
   * Get a psagent id
   * @return psagent id
   * @throws ServiceException
   */
  public int getPSAgentId() throws ServiceException {
    return master.getPSAgentId(null, GetPSAgentIdRequest.getDefaultInstance()).getPsAgentId();
  }

  /**
   * Check PS exist or not
   * @param psLoc ps id and location
   * @return true means ps exited
   */
  public boolean isPSExited(PSLocation psLoc) throws ServiceException {
    return master.checkPSExited(null,
      CheckPSExitRequest.newBuilder().setClientId(PSAgentContext.get().getPSAgentId()).setPsLoc(ProtobufUtil.convert(psLoc)).build()).getExited() == 1;
  }
}
