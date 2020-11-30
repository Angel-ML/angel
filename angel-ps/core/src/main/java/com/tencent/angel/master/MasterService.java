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


package com.tencent.angel.master;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.location.LocationManager;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ipc.MLRPC;
import com.tencent.angel.ipc.RpcServer;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.AppEvent;
import com.tencent.angel.master.app.AppEventType;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.matrix.committer.SaveResult;
import com.tencent.angel.master.matrixmeta.AMMatrixMetaManager;
import com.tencent.angel.master.metrics.MetricsEvent;
import com.tencent.angel.master.metrics.MetricsEventType;
import com.tencent.angel.master.metrics.MetricsUpdateEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptDiagnosticsUpdateEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.ps.attempt.PSAttemptRegisterEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptStateUpdateEvent;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.attempt.WorkerAttemptDiagnosticsUpdateEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.master.worker.attempt.WorkerAttemptRegisterEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptStateUpdateEvent;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupState;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixReport;
import com.tencent.angel.ml.metric.Metric;
import com.tencent.angel.model.LoadState;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelLoadContext;
import com.tencent.angel.model.ModelLoadResult;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.model.ModelSaveResult;
import com.tencent.angel.model.PSMatricesLoadContext;
import com.tencent.angel.model.PSMatricesLoadResult;
import com.tencent.angel.model.PSMatricesSaveContext;
import com.tencent.angel.model.PSMatricesSaveResult;
import com.tencent.angel.model.SaveState;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.CheckModelLoadedRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.CheckModelLoadedResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.CheckModelSavedRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.CheckModelSavedResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.ClientRegisterRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.ClientRegisterResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetClientIdRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetClientIdResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetJobReportRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetJobReportResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetWorkerLogDirRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetWorkerLogDirResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.KeepAliveRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.KeepAliveResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.KillPSRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.KillPSResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.LoadRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.LoadResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.ModelLoadContextProto;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.ModelSaveContextProto;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.PingRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.PingResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.SaveRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.SaveResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.SetParamsRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.SetParamsResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.StartRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.StartResponse;
import com.tencent.angel.protobuf.generated.MLProtos.CheckMatricesCreatedRequest;
import com.tencent.angel.protobuf.generated.MLProtos.CheckMatricesCreatedResponse;
import com.tencent.angel.protobuf.generated.MLProtos.CreateMatricesRequest;
import com.tencent.angel.protobuf.generated.MLProtos.CreateMatricesResponse;
import com.tencent.angel.protobuf.generated.MLProtos.GetAllPSLocationRequest;
import com.tencent.angel.protobuf.generated.MLProtos.GetAllPSLocationResponse;
import com.tencent.angel.protobuf.generated.MLProtos.GetPSLocationReponse;
import com.tencent.angel.protobuf.generated.MLProtos.GetPSLocationRequest;
import com.tencent.angel.protobuf.generated.MLProtos.GetPartLocationRequest;
import com.tencent.angel.protobuf.generated.MLProtos.GetPartLocationResponse;
import com.tencent.angel.protobuf.generated.MLProtos.GetStoredPssRequest;
import com.tencent.angel.protobuf.generated.MLProtos.GetStoredPssResponse;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.MLProtos.PSLocationProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSStatus;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.CheckPSExitRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.CheckPSExitResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetAllMatrixMetaRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetAllMatrixMetaResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetMatricesRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetMatricesResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetPSAgentIdRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetPSAgentIdResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentCommandProto;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentDoneRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentDoneResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentErrorRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentErrorResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentRegisterRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentRegisterResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentReportRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentReportResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSFailedReportRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSFailedReportResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.ReleaseMatricesRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.ReleaseMatricesResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskClockRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskClockResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskIterationRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskIterationResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetCheckpointsRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetCheckpointsResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetExecuteUnitDescRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetExecuteUnitDescResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetIterationRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetIterationResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetPSMatricesMetaRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetPSMatricesResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetSaveResultsRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetSaveResultsResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetTaskMatrixClockRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetTaskMatrixClockResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.LoadFinishRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.LoadFinishResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.LoadStartRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.LoadStartResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.MatrixReportProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSCommandProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSDoneRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSDoneResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSErrorRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSErrorResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSRegisterRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSRegisterResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSReportRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSReportResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.SaveFinishRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.SaveFinishResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.SaveStartRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.SaveStartResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.TaskMatrixClock;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupMetaInfoRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupMetaInfoResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupSuccessNumRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupSuccessNumResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerCommandProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerDoneRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerDoneResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerErrorRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerErrorResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerRegisterRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerRegisterResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerReportRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerReportResponse;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.ha.RecoverPartKey;
import com.tencent.angel.ps.server.data.PSLocation;
import com.tencent.angel.utils.KryoUtils;
import com.tencent.angel.utils.NetUtils;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * the RPC server for angel application master. it respond to requests from clients, worker and ps
 */
public class MasterService extends AbstractService implements MasterProtocol {

  private static final Log LOG = LogFactory.getLog(MasterService.class);
  private final AMContext context;
  /**
   * RPC server
   */
  private RpcServer rpcServer;

  /**
   * heartbeat timeout check thread
   */
  private Thread timeOutChecker;

  private final AtomicBoolean stopped;

  /**
   * received matrix meta from client
   */
  private final List<MatrixMeta> matrics;

  /**
   * host and port of the RPC server
   */
  private volatile Location location;

  /**
   * Yarn web port
   */
  private final int yarnNMWebPort;


  public MasterService(AMContext context) {
    super(MasterService.class.getName());
    this.context = context;
    this.stopped = new AtomicBoolean(false);
    matrics = new ArrayList<>();

    Configuration conf = context.getConf();
    yarnNMWebPort = getYarnNMWebPort(conf);
  }

  private int getYarnNMWebPort(Configuration conf) {
    String nmWebAddr =
        conf.get(YarnConfiguration.NM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_ADDRESS);
    String[] addrItems = nmWebAddr.split(":");
    if (addrItems.length == 2) {
      try {
        return Integer.valueOf(addrItems[1]);
      } catch (Throwable x) {
        LOG.error("can not get nm web port from " + nmWebAddr + ", just return default 8080");
        return 8080;
      }
    } else {
      return 8080;
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 0;
  }

  /**
   * response for parameter server heartbeat
   *
   * @param controller rpc controller of protobuf
   * @param request heartbeat request
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSReportResponse psReport(
      RpcController controller, PSReportRequest request) throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive ps heartbeat request. request=" + request);
    }

    //parse parameter server counters
    List<Pair> params = request.getMetricsList();
    int size = params.size();
    Map<String, String> paramsMap = new HashMap<String, String>();
    for (int i = 0; i < size; i++) {
      paramsMap.put(params.get(i).getKey(), params.get(i).getValue());
    }

    PSAttemptId psAttemptId = ProtobufUtil.convertToId(request.getPsAttemptId());
    PSReportResponse.Builder resBuilder = PSReportResponse.newBuilder();
    if (!context.getParameterServerManager().isAlive(psAttemptId)) {
      //if psAttemptId is not in monitor set, just return a PSCOMMAND_SHUTDOWN command.
      LOG.error("ps attempt " + psAttemptId + " is not in running ps attempt set");
      resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_SHUTDOWN);
    } else {
      resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_OK);
      //refresh last heartbeat timestamp
      context.getParameterServerManager().alive(psAttemptId);

      //send a state update event to the specific PSAttempt
      context.getEventHandler().handle(new PSAttemptStateUpdateEvent(psAttemptId, paramsMap));

      // Check is there save request
      PSMatricesSaveContext subSaveContext =
          context.getModelSaver().getSaveContext(psAttemptId.getPsId());
      PSMatricesSaveResult subSaveResult =
          context.getModelSaver().getSaveResult(psAttemptId.getPsId());
      if (subSaveContext != null && subSaveResult != null && (subSaveContext.getRequestId()
          == subSaveResult.getRequestId()) && (subSaveResult.getState() == SaveState.INIT
          || subSaveResult.getState() == SaveState.SAVING)) {
        //LOG.info("PS " + psAttemptId + " need save " + subSaveContext);
        resBuilder.setNeedSaveMatrices(ProtobufUtil.convert(subSaveContext));
      }

      // Check is there load request
      PSMatricesLoadContext subLoadContext =
          context.getModelLoader().getLoadContext(psAttemptId.getPsId());
      PSMatricesLoadResult subLoadResult =
          context.getModelLoader().getLoadResult(psAttemptId.getPsId());
      if (subLoadContext != null && subLoadResult != null
          && subLoadContext.getRequestId() == subLoadResult.getRequestId()
          && (subLoadResult.getState() == LoadState.INIT
          || subLoadResult.getState() == LoadState.LOADING)) {
        //LOG.info("PS " + psAttemptId + " need load " + subLoadContext);
        resBuilder.setNeedLoadMatrices(ProtobufUtil.convert(subLoadContext));
      }

      //check matrix metadata inconsistencies between master and parameter server.
      //if a matrix exists on the Master and does not exist on ps, then it is necessary to notify ps to establish the matrix
      //if a matrix exists on the ps and does not exist on master, then it is necessary to notify ps to remove the matrix
      List<MatrixReportProto> matrixReportsProto = request.getMatrixReportsList();
      List<Integer> needReleaseMatrices = new ArrayList<>();
      List<MatrixMeta> needCreateMatrices = new ArrayList<>();
      List<RecoverPartKey> needRecoverParts = new ArrayList<>();

      List<MatrixReport> matrixReports = ProtobufUtil.convertToMatrixReports(matrixReportsProto);
      context.getMatrixMetaManager()
          .syncMatrixInfos(matrixReports, needCreateMatrices, needReleaseMatrices, needRecoverParts,
              psAttemptId.getPsId());

      size = needCreateMatrices.size();
      for (int i = 0; i < size; i++) {
        resBuilder
            .addNeedCreateMatrices(
                ProtobufUtil.convertToMatrixMetaProto(needCreateMatrices.get(i)));
      }

      size = needReleaseMatrices.size();
      for (int i = 0; i < size; i++) {
        resBuilder.addNeedReleaseMatrixIds(needReleaseMatrices.get(i));
      }

      size = needRecoverParts.size();
      for (int i = 0; i < size; i++) {
        resBuilder.addNeedRecoverParts(ProtobufUtil.convert(needRecoverParts.get(i)));
      }
    }

    return resBuilder.build();
  }

  @Override
  public PSAgentMasterServiceProtos.FetchMinClockResponse fetchMinClock(RpcController controller,
      PSAgentMasterServiceProtos.FetchMinClockRequest request) {
    return PSAgentMasterServiceProtos.FetchMinClockResponse.newBuilder().setMinClock(10).build();
  }

  /**
   * response for parameter server register.
   *
   * @param controller rpc controller of protobuf
   * @param request register request
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSRegisterResponse psRegister(
      RpcController controller, PSRegisterRequest request) throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive ps register request. request=" + request);
    }

    PSAttemptId psAttemptId = ProtobufUtil.convertToId(request.getPsAttemptId());
    PSRegisterResponse.Builder resBuilder = PSRegisterResponse.newBuilder();

    //if psAttemptId is not in monitor set, just return a PSCOMMAND_SHUTDOWN command.
    if (!context.getParameterServerManager().isAlive(psAttemptId)) {
      LOG.info(psAttemptId + " doesn't exists!");
      resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_SHUTDOWN);
    } else {
      context.getParameterServerManager().alive(psAttemptId);
      context.getEventHandler().handle(new PSAttemptRegisterEvent(psAttemptId,
          new Location(request.getLocation().getIp(), request.getLocation().getPort())));
      LOG.info(psAttemptId + " is registered now!");
      resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_OK);
    }
    LOG.info(psAttemptId + " register finished!");
    return resBuilder.build();
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override protected void serviceInit(Configuration conf) throws Exception {
    String ip;
    int servicePort;
    if(conf.get(AngelConf.ANGEL_DEPLOY_MODE, AngelConf.DEFAULT_ANGEL_DEPLOY_MODE).equals("KUBERNETES")) {
      ip = conf.get(AngelConf.ANGEL_KUBERNETES_MASTER_POD_IP);
      servicePort = conf.getInt(AngelConf.ANGEL_KUBERNETES_MASTER_PORT,
              AngelConf.DEFAULT_ANGEL_KUBERNETES_MASTER_PORT);
    } else {
      ip = NetUtils.getRealLocalIP();
      //choose a unused port
      servicePort = NetUtils.chooseAListenPort(conf);
    }
    LOG.info("listen ip:" + ip + ", port:" + servicePort);

    location = new Location(ip, servicePort);
    //start RPC server
    this.rpcServer = MLRPC
            .getServer(MasterService.class, this, new Class<?>[] {MasterProtocol.class}, ip, servicePort,
                    conf);
    rpcServer.openServer();
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (!stopped.getAndSet(true)) {
      if (rpcServer != null) {
        rpcServer.stop();
        rpcServer = null;
      }

      if (timeOutChecker != null) {
        timeOutChecker.interrupt();
        try {
          timeOutChecker.join();
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException while stopping", ie);
        }
        timeOutChecker = null;
      }
    }

    super.serviceStop();
    LOG.info("WorkerPSService is stoped!");
  }

  public RpcServer getRpcServer() {
    return rpcServer;
  }

  /**
   * get application state.
   *
   * @param controller rpc controller of protobuf
   * @param request request
   */
  @Override
  public GetJobReportResponse getJobReport(RpcController controller,
      GetJobReportRequest request) throws ServiceException {
    GetJobReportResponse response = context.getApp().getJobReportResponse();
    return response;
  }

  /**
   * Get worker log url
   *
   * @param controller rpc controller
   * @param request rpc request contains worker id
   * @return worker log url
   * @throws ServiceException worker does not exist
   */
  @Override
  public GetWorkerLogDirResponse getWorkerLogDir(RpcController controller,
      GetWorkerLogDirRequest request) throws ServiceException {
    WorkerId workerId = ProtobufUtil.convertToId(request.getWorkerId());
    AMWorker worker = context.getWorkerManager().getWorker(workerId);
    if (worker == null) {
      throw new ServiceException("can not find worker " + workerId);
    }

    WorkerAttempt workerAttempt = worker.getRunningAttempt();
    if (workerAttempt == null) {
      return GetWorkerLogDirResponse.newBuilder().setLogDir("").build();
    }

    Location loc = workerAttempt.getLocation();
    Container container = workerAttempt.getContainer();
    if (loc == null || container == null) {
      return GetWorkerLogDirResponse.newBuilder().setLogDir("").build();
    }

    return GetWorkerLogDirResponse.newBuilder().setLogDir(
        "http://" + loc.getIp() + ":" + yarnNMWebPort + "/node/containerlogs/" + container.getId()
            + "/angel/syslog/?start=0").build();
  }

  /**
   * set matrix meta
   *
   * @param controller rpc controller of protobuf
   * @param request matrix meta
   */
  @Override
  public CreateMatricesResponse createMatrices(RpcController controller,
      CreateMatricesRequest request) throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive create matrix request. request=" + request);
    }

    try {
      context.getMatrixMetaManager()
          .createMatrices(ProtobufUtil.convertToMatrixContexts(request.getMatricesList()));
    } catch (Throwable e) {
      throw new ServiceException(e);
    }

    try {
      context.getAppStateStorage().writeMatrixMeta(context.getMatrixMetaManager());
    } catch (Exception e) {
      LOG.error("write matrix meta to file failed.", e);
    }
    return CreateMatricesResponse.newBuilder().build();
  }

  /**
   * Get matrices metadata
   */
  @Override
  public GetMatricesResponse getMatrices(RpcController controller,
      GetMatricesRequest request) throws ServiceException {
    GetMatricesResponse.Builder builder = GetMatricesResponse.newBuilder();
    AMMatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();

    List<String> matrixNames = request.getMatrixNamesList();
    int size = matrixNames.size();
    for (int i = 0; i < size; i++) {
      MatrixMeta matrixMeta = matrixMetaManager.getMatrix(matrixNames.get(i));
      if (matrixMeta == null) {
        throw new ServiceException("Can not find matrix " + matrixNames.get(i));
      }
      builder.addMatrixMetas(ProtobufUtil.convertToMatrixMetaProto(matrixMeta));
    }
    return builder.build();
  }

  /**
   * Release matrices
   */
  @Override
  public ReleaseMatricesResponse releaseMatrices(RpcController controller,
      ReleaseMatricesRequest request) throws ServiceException {
    AMMatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();
    List<String> matrixNames = request.getMatrixNamesList();

    int size = matrixNames.size();
    for (int i = 0; i < size; i++) {
      matrixMetaManager.releaseMatrix(matrixNames.get(i));
    }
    return ReleaseMatricesResponse.newBuilder().build();
  }

  public InetSocketAddress getRPCListenAddr() {
    return rpcServer.getListenerAddress();
  }

  /**
   * notify a parameter server run over successfully
   *
   * @param controller rpc controller of protobuf
   * @param request parameter server attempt id
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSDoneResponse psDone(RpcController controller,
      PSDoneRequest request) throws ServiceException {
    PSAttemptId psAttemptId = ProtobufUtil.convertToId(request.getPsAttemptId());
    LOG.info("psAttempt " + psAttemptId + " is done");

    //remove this parameter server attempt from monitor set
    context.getParameterServerManager().unRegister(psAttemptId);

    context.getEventHandler()
        .handle(new PSAttemptEvent(PSAttemptEventType.PA_SUCCESS, psAttemptId));
    return PSDoneResponse.newBuilder().build();
  }

  /**
   * notify a parameter server run failed
   *
   * @param controller rpc controller of protobuf
   * @param request contains parameter server id and error message
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSErrorResponse psError(RpcController controller,
      PSErrorRequest request) throws ServiceException {
    PSAttemptId psAttemptId = ProtobufUtil.convertToId(request.getPsAttemptId());
    LOG.info("error happened in psAttempt " + psAttemptId + " error msg=" + request.getMsg());

    //remove this parameter server attempt from monitor set
    context.getParameterServerManager().unRegister(psAttemptId);

    context.getEventHandler()
        .handle(new PSAttemptDiagnosticsUpdateEvent(request.getMsg(), psAttemptId));

    context.getEventHandler()
        .handle(new PSAttemptEvent(PSAttemptEventType.PA_FAILMSG, psAttemptId));

    return PSErrorResponse.newBuilder().build();
  }

  /**
   * get all matrix meta
   *
   * @param controller rpc controller of protobuf
   */
  @Override
  public GetAllMatrixMetaResponse getAllMatrixMeta(RpcController controller,
      GetAllMatrixMetaRequest request) throws ServiceException {

    GetAllMatrixMetaResponse.Builder resBuilder = GetAllMatrixMetaResponse.newBuilder();
    Map<Integer, MatrixMeta> matrixIdToMetaMap = context.getMatrixMetaManager().getMatrixMetas();

    for (Entry<Integer, MatrixMeta> metaEntry : matrixIdToMetaMap.entrySet()) {
      resBuilder.addMatrixMetas(ProtobufUtil.convertToMatrixMetaProto(metaEntry.getValue()));
    }
    return resBuilder.build();
  }

  /**
   * get all parameter server locations.
   *
   * @param controller rpc controller of protobuf
   */
  @Override
  public GetAllPSLocationResponse getAllPSLocation(RpcController controller,
      GetAllPSLocationRequest request) {
    GetAllPSLocationResponse.Builder resBuilder = GetAllPSLocationResponse.newBuilder();
    LocationManager locationManager = context.getLocationManager();
    ParameterServerId[] psIds = locationManager.getPsIds();
    for (int i = 0; i < psIds.length; i++) {
      resBuilder.addPsLocations(
          ProtobufUtil.convertToPSLocProto(psIds[i], locationManager.getPsLocation(psIds[i])));
    }
    return resBuilder.build();
  }

  /**
   * get a specific parameter server location.
   *
   * @param controller rpc controller of protobuf
   * @param request parameter server id
   */
  @Override
  public GetPSLocationReponse getPSLocation(RpcController controller,
      GetPSLocationRequest request) throws ServiceException {
    GetPSLocationReponse.Builder resBuilder = GetPSLocationReponse.newBuilder();
    ParameterServerId psId = ProtobufUtil.convertToId(request.getPsId());

    Location psLocation = context.getLocationManager().getPsLocation(psId);
    if (psLocation == null) {
      resBuilder.setPsLocation(
          PSLocationProto.newBuilder().setPsId(request.getPsId()).setPsStatus(PSStatus.PS_NOTREADY)
              .build());
    } else {
      resBuilder.setPsLocation(ProtobufUtil.convertToPSLocProto(psId, psLocation));
    }
    return resBuilder.build();
  }

  /**
   * Get locations for a partition
   */
  @Override
  public GetPartLocationResponse getPartLocation(RpcController controller,
      GetPartLocationRequest request) throws ServiceException {
    GetPartLocationResponse.Builder builder = GetPartLocationResponse.newBuilder();
    List<ParameterServerId> psIds =
        context.getMatrixMetaManager().getPss(request.getMatrixId(), request.getPartId());

    if (psIds != null) {
      int size = psIds.size();
      for (int i = 0; i < size; i++) {
        Location psLocation = context.getLocationManager().getPsLocation(psIds.get(i));
        if (psLocation == null) {
          builder.addLocations(
              (PSLocationProto.newBuilder().setPsId(ProtobufUtil.convertToIdProto(psIds.get(i)))
                  .setPsStatus(PSStatus.PS_NOTREADY).build()));
        } else {
          builder.addLocations(ProtobufUtil.convertToPSLocProto(psIds.get(i), psLocation));
        }
      }
    }

    return builder.build();
  }

  /**
   * Get iteration now
   */
  @Override
  public GetIterationResponse getIteration(RpcController controller,
      GetIterationRequest request) throws ServiceException {
    int curIteration = 0;
    if (context.getAlgoMetricsService() != null) {
      curIteration = context.getAlgoMetricsService().getCurrentIter();
    }
    return GetIterationResponse.newBuilder().setIteration(curIteration).build();
  }

  @Override
  public GetPSMatricesResponse getPSMatricesMeta(RpcController controller,
      GetPSMatricesMetaRequest request) throws ServiceException {
    Map<Integer, MatrixMeta> matrixIdToMetaMap = context.getMatrixMetaManager()
        .getMatrixPartitions(ProtobufUtil.convertToId(request.getPsId()));
    GetPSMatricesResponse.Builder builder = GetPSMatricesResponse.newBuilder();
    if (matrixIdToMetaMap != null && !matrixIdToMetaMap.isEmpty()) {
      for (MatrixMeta meta : matrixIdToMetaMap.values()) {
        builder.addMatricesMeta(ProtobufUtil.convertToMatrixMetaProto(meta));
      }
    }
    return builder.build();
  }

  /**
   * PS report save request finish
   */
  @Override
  public SaveFinishResponse saveFinish(RpcController controller,
      SaveFinishRequest request) throws ServiceException {
    LOG.info("save finish request=" + request);
    context.getModelSaver()
        .psSaveFinish(ProtobufUtil.convertToId(request.getPsAttemptId()).getPsId(),
            ProtobufUtil.convert(request.getResult()));
    return SaveFinishResponse.getDefaultInstance();
  }

  /**
   * PS report load finish
   */
  @Override
  public LoadFinishResponse loadFinish(RpcController controller,
      LoadFinishRequest request) throws ServiceException {
    LOG.info("load finish request=" + request);
    context.getModelLoader()
        .psLoadFinish(ProtobufUtil.convertToId(request.getPsAttemptId()).getPsId(),
            ProtobufUtil.convert(request.getResult()));
    return LoadFinishResponse.getDefaultInstance();
  }

  /**
   * PS report save start
   */
  @Override
  public SaveStartResponse saveStart(RpcController controller, SaveStartRequest request)
      throws ServiceException {
    LOG.info("save start request=" + request);
    context.getModelSaver()
        .psSaveStart(ProtobufUtil.convertToId(request.getPsAttemptId()).getPsId(),
            request.getRequestId(), request.getSubRequestId());
    return SaveStartResponse.getDefaultInstance();
  }

  /**
   * PS report load start
   */
  @Override
  public LoadStartResponse loadStart(RpcController controller, LoadStartRequest request)
      throws ServiceException {
    LOG.info("load start request=" + request);
    context.getModelLoader()
        .psLoadStart(ProtobufUtil.convertToId(request.getPsAttemptId()).getPsId(),
            request.getRequestId(), request.getSubRequestId());
    return LoadStartResponse.getDefaultInstance();
  }

  /**
   * Get model save results
   */
  @Override
  public GetSaveResultsResponse getSaveResults(RpcController controller,
      GetSaveResultsRequest request) throws ServiceException {
    LOG.info("Get save result request=" + request);
    List<SaveResult> results = context.getModelSaver().getSaveResults(request.getMatrixId());

    if (results == null || results.isEmpty()) {
      return GetSaveResultsResponse.getDefaultInstance();
    }

    GetSaveResultsResponse.Builder builder = GetSaveResultsResponse.newBuilder();
    for (SaveResult result : results) {
      builder.addSaveResults(ProtobufUtil.convert(result));
    }
    return builder.build();
  }

  /**
   * Get checkpoint results
   */
  @Override
  public GetCheckpointsResponse getCheckpointResults(RpcController controller,
      GetCheckpointsRequest request) throws ServiceException {
    LOG.info("Get checkpoint result request=" + request);
    List<SaveResult> results = context.getModelSaver().getCheckpointResults(request.getMatrixId());

    if (results == null || results.isEmpty()) {
      return GetCheckpointsResponse.getDefaultInstance();
    }

    GetCheckpointsResponse.Builder builder = GetCheckpointsResponse.newBuilder();
    for (SaveResult result : results) {
      builder.addSaveResults(ProtobufUtil.convert(result));
    }
    return builder.build();
  }

  /**
   * Get the stored pss of a matrix partition
   */
  @Override
  public GetStoredPssResponse getStoredPss(RpcController controller,
      GetStoredPssRequest request) throws ServiceException {
    GetStoredPssResponse.Builder builder = GetStoredPssResponse.newBuilder();
    List<ParameterServerId> psIds =
        context.getMatrixMetaManager().getPss(request.getMatrixId(), request.getMatrixId());

    if (psIds != null) {
      int size = psIds.size();
      for (int i = 0; i < size; i++) {
        builder.addPsIds(ProtobufUtil.convertToIdProto(psIds.get(i)));
      }
    }
    return builder.build();
  }

  /**
   * Get a new psagent id
   */
  @Override
  public GetPSAgentIdResponse getPSAgentId(RpcController controller,
      GetPSAgentIdRequest request) throws ServiceException {
    return GetPSAgentIdResponse.newBuilder().setPsAgentId(context.getPSAgentManager().getId())
        .build();
  }

  /**
   * Check PS exited or not
   */
  @Override
  public CheckPSExitResponse checkPSExited(RpcController controller,
      CheckPSExitRequest request) throws ServiceException {
    if (context.getParameterServerManager().checkFailed(ProtobufUtil.convert(request.getPsLoc()))) {
      return CheckPSExitResponse.newBuilder().setExited(1).build();
    } else {
      return CheckPSExitResponse.newBuilder().setExited(0).build();
    }
  }


  /**
   * response for psagent heartbeat.
   *
   * @param controller rpc controller of protobuf
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSAgentReportResponse psAgentReport(
      RpcController controller, PSAgentReportRequest request) throws ServiceException {
    return PSAgentReportResponse.newBuilder().build();
  }

  /**
   * response for psagent heartbeat.
   *
   * @param controller rpc controller of protobuf
   * @param request contains psagent attempt id
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSAgentRegisterResponse psAgentRegister(
      RpcController controller, PSAgentRegisterRequest request) throws ServiceException {
    LOG.info("PSAgent register:" + request);
    return PSAgentRegisterResponse.newBuilder().setCommand(PSAgentCommandProto.PSAGENT_SUCCESS)
        .build();
  }

  /**
   * psagent run over successfully
   *
   * @param controller rpc controller of protobuf
   * @param request contains psagent attempt id
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSAgentDoneResponse psAgentDone(
      RpcController controller, PSAgentDoneRequest request) throws ServiceException {
    PSAgentDoneResponse.Builder resBuilder = PSAgentDoneResponse.newBuilder();
    return resBuilder.build();
  }

  /**
   * psagent run falied
   *
   * @param controller rpc controller of protobuf
   * @param request contains psagent attempt id, error message
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSAgentErrorResponse psAgentError(
      RpcController controller, PSAgentErrorRequest request) throws ServiceException {
    PSAgentErrorResponse.Builder resBuilder = PSAgentErrorResponse.newBuilder();
    return resBuilder.build();
  }

  @Override
  public GetExecuteUnitDescResponse getExecuteUnitDesc(RpcController controller,
      GetExecuteUnitDescRequest request) throws ServiceException {
    return null;
  }

  /**
   * response for worker heartbeat
   *
   * @param controller rpc controller of protobuf
   * @param request contains worker attempt id, task metrics
   */
  @SuppressWarnings("unchecked")
  @Override
  public WorkerReportResponse workerReport(
      RpcController controller, WorkerReportRequest request) throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive worker report, request=" + request);
    }

    WorkerAttemptId workerAttemptId = ProtobufUtil.convertToId(request.getWorkerAttemptId());
    if (!context.getWorkerManager().isAlive(workerAttemptId)) {
      LOG.error("worker attempt " + workerAttemptId
          + " is not in running worker attempt set now, shutdown it");
      return WorkerReportResponse.newBuilder().setCommand(WorkerCommandProto.W_SHUTDOWN).build();
    } else {
      context.getEventHandler().handle(new WorkerAttemptStateUpdateEvent(workerAttemptId, request));
      context.getWorkerManager().alive(workerAttemptId);
      return WorkerReportResponse.newBuilder()
          .setActiveTaskNum(context.getWorkerManager().getActiveTaskNum())
          .setCommand(WorkerCommandProto.W_SUCCESS).build();
    }
  }

  /**
   * worker register to master
   *
   * @param controller rpc controller of protobuf
   * @param request contains worker attempt id, worker location
   */
  @SuppressWarnings("unchecked")
  @Override
  public WorkerRegisterResponse workerRegister(
      RpcController controller, WorkerRegisterRequest request) throws ServiceException {
    WorkerRegisterResponse.Builder registerResponseBuilder = WorkerRegisterResponse.newBuilder();
    WorkerAttemptId workerAttemptId = ProtobufUtil.convertToId(request.getWorkerAttemptId());

    LOG.info(
        "Worker " + workerAttemptId + " register, location=" + request.getLocation()
            + ", psagent id="
            + request.getPsAgentId());
    //if worker attempt id is not in monitor set, we should shutdown it
    if (!context.getWorkerManager().isAlive(workerAttemptId)) {
      LOG.error("worker attempt " + workerAttemptId
          + " is not in running worker attempt set now, shutdown it");
      registerResponseBuilder.setCommand(WorkerCommandProto.W_SHUTDOWN);
    } else {
      context.getWorkerManager().alive(workerAttemptId);
      Location location =
          new Location(request.getLocation().getIp(), request.getLocation().getPort());
      context.getEventHandler().handle(new WorkerAttemptRegisterEvent(workerAttemptId, location));
      registerResponseBuilder.setCommand(WorkerCommandProto.W_SUCCESS);

      LOG.info("worker attempt " + workerAttemptId + " register finished!");
    }

    return registerResponseBuilder.build();
  }

  /**
   * get worker group information: tasks, workers, data splits
   *
   * @param controller rpc controller of protobuf
   * @param request contains worker attempt id
   */
  @Override
  public GetWorkerGroupMetaInfoResponse getWorkerGroupMetaInfo(RpcController controller,
      GetWorkerGroupMetaInfoRequest request) throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive get workergroup info, request=" + request);
    }
    WorkerAttemptId workerAttemptId = ProtobufUtil.convertToId(request.getWorkerAttemptId());

    //find workergroup in worker manager
    AMWorkerGroup group =
        context.getWorkerManager().getWorkerGroup(workerAttemptId.getWorkerId().getWorkerGroupId());

    if (group == null || group.getState() == AMWorkerGroupState.NEW
        || group.getState() == AMWorkerGroupState.INITED) {
      //if this worker group does not initialized, just return WORKERGROUP_NOTREADY
      return GetWorkerGroupMetaInfoResponse.newBuilder()
          .setWorkerGroupStatus(
              GetWorkerGroupMetaInfoResponse.WorkerGroupStatus.WORKERGROUP_NOTREADY)
          .build();
    } else if (group.getState() == AMWorkerGroupState.FAILED
        || group.getState() == AMWorkerGroupState.KILLED
        || group.getState() == AMWorkerGroupState.SUCCESS) {
      //if this worker group run over, just return WORKERGROUP_EXITED
      return GetWorkerGroupMetaInfoResponse.newBuilder()
          .setWorkerGroupStatus(GetWorkerGroupMetaInfoResponse.WorkerGroupStatus.WORKERGROUP_EXITED)
          .build();
    } else {
      //if this worker group is running now, return tasks, workers, data splits for it
      try {
        return ProtobufUtil.buildGetWorkerGroupMetaResponse(group,
            context.getDataSpliter().getSplits(group.getSplitIndex()), context.getConf());
      } catch (Exception e) {
        LOG.error("build workergroup information error", e);
        throw new ServiceException(e);
      }
    }
  }

  /**
   * worker run over successfully
   *
   * @param controller rpc controller of protobuf
   * @param request contains worker attempt id
   */
  @SuppressWarnings("unchecked")
  @Override
  public WorkerDoneResponse workerDone(
      RpcController controller, WorkerDoneRequest request) throws ServiceException {
    WorkerAttemptId workerAttemptId = ProtobufUtil.convertToId(request.getWorkerAttemptId());
    LOG.info("worker attempt " + workerAttemptId + " is done");
    WorkerDoneResponse.Builder resBuilder = WorkerDoneResponse.newBuilder();

    //if worker attempt id is not in monitor set, we should shutdown it
    if (!context.getWorkerManager().isAlive(workerAttemptId)) {
      resBuilder.setCommand(WorkerCommandProto.W_SHUTDOWN);
    } else {
      context.getWorkerManager().unRegister(workerAttemptId);
      resBuilder.setCommand(WorkerCommandProto.W_SUCCESS);
      context.getEventHandler()
          .handle(new WorkerAttemptEvent(WorkerAttemptEventType.DONE, workerAttemptId));
    }

    return resBuilder.build();
  }

  /**
   * worker run failed
   *
   * @param controller rpc controller of protobuf
   * @param request contains worker attempt id, error message
   */
  @SuppressWarnings("unchecked")
  @Override
  public WorkerErrorResponse workerError(
      RpcController controller, WorkerErrorRequest request) throws ServiceException {
    WorkerAttemptId workerAttemptId = ProtobufUtil.convertToId(request.getWorkerAttemptId());
    LOG.info("worker attempt " + workerAttemptId + " failed, details=" + request.getMsg());

    WorkerErrorResponse.Builder resBuilder = WorkerErrorResponse.newBuilder();

    //if worker attempt id is not in monitor set, we should shutdown it
    if (!context.getWorkerManager().isAlive(workerAttemptId)) {
      resBuilder.setCommand(WorkerCommandProto.W_SHUTDOWN);
    } else {
      context.getWorkerManager().unRegister(workerAttemptId);
      context.getEventHandler()
          .handle(new WorkerAttemptDiagnosticsUpdateEvent(workerAttemptId, request.getMsg()));
      context.getEventHandler()
          .handle(new WorkerAttemptEvent(WorkerAttemptEventType.ERROR, workerAttemptId));
      resBuilder.setCommand(WorkerCommandProto.W_SUCCESS);
    }

    return resBuilder.build();
  }

  /**
   * Get success Worker group number
   *
   * @param controller rpc controller of protobuf
   * @param request empty
   * @return success Worker group number
   */
  @Override
  public GetWorkerGroupSuccessNumResponse getWorkerGroupSuccessNum(
      RpcController controller, GetWorkerGroupSuccessNumRequest request) throws ServiceException {
    return GetWorkerGroupSuccessNumResponse.newBuilder()
        .setSuccessNum(context.getWorkerManager().getSuccessWorkerGroupNum()).build();
  }

  public List<MatrixMeta> getMatrics() {
    return matrics;
  }

  /**
   * task update the clock for a matrix
   *
   * @param controller rpc controller of protobuf
   * @param request contains task id, matrix id and clock value
   */
  @Override
  public TaskClockResponse taskClock(RpcController controller, TaskClockRequest request)
      throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive task clock, request=" + request);
    }

    TaskId taskId = ProtobufUtil.convertToId(request.getTaskId());

    //get Task meta from task manager, if can not find, just new a AMTask object and put it to task manager
    //in ANGEL_PS mode, task id may can not know advance
    AMTask task = context.getTaskManager().getTask(taskId);
    if (task == null) {
      task = new AMTask(taskId, null);
      context.getTaskManager().putTask(taskId, task);
    }

    //update the clock for this matrix
    task.clock(request.getMatrixClock().getMatrixId(), request.getMatrixClock().getClock());
    return TaskClockResponse.newBuilder().build();
  }

  /**
   * task update iteration number
   *
   * @param controller rpc controller of protobuf
   * @param request contains task id, iteration number
   */
  @Override
  public TaskIterationResponse taskIteration(RpcController controller,
      TaskIterationRequest request) throws ServiceException {
    LOG.debug("task iteration, " + request);
    TaskId taskId = ProtobufUtil.convertToId(request.getTaskId());

    //get Task meta from task manager, if can not find, just new a AMTask object and put it to task manager
    //in ANGEL_PS mode, task id may can not know advance
    AMTask task = context.getTaskManager().getTask(taskId);
    if (task == null) {
      task = new AMTask(taskId, null);
      context.getTaskManager().putTask(taskId, task);
    }

    //update task iteration
    task.iteration(request.getIteration());
    context.getEventHandler().handle(new MetricsEvent(MetricsEventType.TASK_ITERATION_UPDATE));
    return TaskIterationResponse.newBuilder().build();
  }

  @Override
  public PSAgentMasterServiceProtos.TaskCountersUpdateResponse taskCountersUpdate(
      RpcController controller, PSAgentMasterServiceProtos.TaskCounterUpdateRequest request)
      throws ServiceException {
    AMTask task = context.getTaskManager().getTask(ProtobufUtil.convertToId(request.getTaskId()));
    if (task != null) {
      task.updateCounters(request.getCountersList());
    }
    return PSAgentMasterServiceProtos.TaskCountersUpdateResponse.newBuilder().build();
  }

  /**
   * Set algorithm metrics
   *
   * @param controller rpc controller of protobuf
   * @param request request contains algorithm metrics of a task
   */
  @Override
  public PSAgentMasterServiceProtos.SetAlgoMetricsResponse setAlgoMetrics(
      RpcController controller, PSAgentMasterServiceProtos.SetAlgoMetricsRequest request)
      throws ServiceException {
    List<PSAgentMasterServiceProtos.AlgoMetric> metrics = request.getAlgoMetricsList();
    int size = metrics.size();
    Map<String, Metric> nameToMetricMap = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      nameToMetricMap.put(metrics.get(i).getName(),
          KryoUtils.deserializeAlgoMetric(metrics.get(i).getSerializedMetric().toByteArray()));
    }
    context.getEventHandler().handle(new MetricsUpdateEvent(nameToMetricMap));
    return PSAgentMasterServiceProtos.SetAlgoMetricsResponse.newBuilder().build();
  }

  @Override
  public PSFailedReportResponse psFailedReport(RpcController controller,
      PSFailedReportRequest request) throws ServiceException {
    LOG.info("Receive client ps failed report " + request);
    PSLocation psLoc = ProtobufUtil.convert(request.getPsLoc());
    context.getParameterServerManager().psFailedReport(psLoc);
    return PSFailedReportResponse.newBuilder().build();
  }

  /**
   * get clock of all matrices for all task
   *
   * @param controller rpc controller of protobuf
   * @param request contains task id
   */
  @Override
  public GetTaskMatrixClockResponse getTaskMatrixClocks(RpcController controller,
      GetTaskMatrixClockRequest request) throws ServiceException {
    AMTaskManager taskManager = context.getTaskManager();
    Collection<AMTask> tasks = taskManager.getTasks();
    GetTaskMatrixClockResponse.Builder builder = GetTaskMatrixClockResponse.newBuilder();
    TaskMatrixClock.Builder taskBuilder = TaskMatrixClock.newBuilder();
    MatrixClock.Builder matrixClockBuilder = MatrixClock.newBuilder();

    Int2IntOpenHashMap matrixClocks = null;
    for (AMTask task : tasks) {
      taskBuilder.setTaskId(ProtobufUtil.convertToIdProto(task.getTaskId()));
      matrixClocks = task.getMatrixClocks();
      for (it.unimi.dsi.fastutil.ints.Int2IntMap.Entry entry : matrixClocks.int2IntEntrySet()) {
        taskBuilder.addMatrixClocks(
            matrixClockBuilder.setMatrixId(entry.getIntKey()).setClock(entry.getIntValue())
                .build());
      }
      builder.addTaskMatrixClocks(taskBuilder.build());
      taskBuilder.clear();
    }

    return builder.build();
  }

  /**
   * use to check a RPC connection to master is established
   *
   * @param controller rpc controller of protobuf
   * @param request a empty request
   */
  @Override
  public PingResponse ping(RpcController controller, PingRequest request)
      throws ServiceException {
    return PingResponse.newBuilder().build();
  }

  public Location getLocation() {
    return location;
  }


  /**
   * Start executing.
   *
   * @param controller rpc controller of protobuf
   * @param request start request
   */
  @Override public StartResponse start(RpcController controller, StartRequest request)
          throws ServiceException {
    LOG.info("start to calculation");
    context.getApp().startExecute();
    if (context.getDeployMode() == AngelDeployMode.KUBERNETES && context.getRunningMode() == RunningMode.ANGEL_PS_WORKER) {
      int workerNum = context.getConf().getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
              AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER);
      while (context.getWorkerManager().getRegisterWorkerNumber() < workerNum) {
        LOG.debug("waiting for worker register in monitor...");
      }
      LOG.info("Now scheduler and lanuch worker pod.");
      Configuration workerConf = new Configuration(context.getConf());
      workerConf.set(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE, "worker");
      context.getK8sClusterManager().scheduler(workerConf);
    }
    return StartResponse.newBuilder().build();
  }


  /**
   * Save model to files.
   *
   * @param controller rpc controller of protobuf
   * @param request save request that contains all matrices need save
   * @throws ServiceException some matrices do not exist or save operation is interrupted
   */
  @SuppressWarnings("unchecked")
  @Override
  public SaveResponse save(RpcController controller,
      SaveRequest request) throws ServiceException {
    ModelSaveContextProto saveContextProto = request.getSaveContext();
    ModelSaveContext saveContext = ProtobufUtil.convert(saveContextProto);
    List<MatrixSaveContext> needSaveMatrices = saveContext.getMatricesContext();
    int size = needSaveMatrices.size();
    for (int i = 0; i < size; i++) {
      if (!context.getMatrixMetaManager().exist(needSaveMatrices.get(i).getMatrixName())) {
        throw new ServiceException(
            "matrix " + needSaveMatrices.get(i).getMatrixName() + " does not exist");
      }
    }

    int requestId;
    try {
      requestId = context.getModelSaver().save(saveContext);
    } catch (Throwable x) {
      throw new ServiceException(x);
    }
    return SaveResponse.newBuilder().setRequestId(requestId).build();
  }

  /**
   * Load model from file
   */
  @Override
  public LoadResponse load(RpcController controller, LoadRequest request)
      throws ServiceException {
    if (context.getModelLoader().isLoading()) {
      throw new ServiceException("Model is loading now, please wait");
    }

    ModelLoadContextProto loadContextProto = request.getLoadContext();
    ModelLoadContext loadContext = ProtobufUtil.convert(loadContextProto);
    List<MatrixLoadContext> needLoadMatrices = loadContext.getMatricesContext();
    int size = needLoadMatrices.size();
    for (int i = 0; i < size; i++) {
      if (!context.getMatrixMetaManager().exist(needLoadMatrices.get(i).getMatrixName())) {
        throw new ServiceException(
            "matrix " + needLoadMatrices.get(i).getMatrixName() + " does not exist");
      }
    }

    int requestId;
    try {
      requestId = context.getModelLoader().load(loadContext);
    } catch (Throwable x) {
      throw new ServiceException(x);
    }
    return LoadResponse.newBuilder().setRequestId(requestId).build();
  }

  /**
   * Set the application to a given finish state
   *
   * @param controller rpc controller
   * @param request application finish state
   * @return response
   */
  @Override
  public ClientMasterServiceProtos.StopResponse stop(RpcController controller,
      ClientMasterServiceProtos.StopRequest request) throws ServiceException {
    LOG.info("receive stop command from client, request=" + request);
    stop(request.getExitStatus());
    return ClientMasterServiceProtos.StopResponse.newBuilder().build();
  }

  public void stop(int exitStatus) {
    switch (exitStatus) {
      case 1: {
        context.getEventHandler().handle(new AppEvent(AppEventType.KILL));
        break;
      }
      case 2: {
        context.getEventHandler().handle(new InternalErrorEvent(context.getApplicationId(),
            "stop the application with failed status"));
        break;
      }
      default: {
        context.getEventHandler().handle(new AppEvent(AppEventType.SUCCESS));
      }
    }
  }

  /**
   * Check matrices are created successfully
   *
   * @param controller rpc controller of protobuf
   * @param request check request that contains matrix names
   */
  @Override
  public CheckMatricesCreatedResponse checkMatricesCreated(RpcController controller,
      CheckMatricesCreatedRequest request) throws ServiceException {
    LOG.info("check matrix created request = " + request);
    List<String> names = request.getMatrixNamesList();
    CheckMatricesCreatedResponse.Builder builder = CheckMatricesCreatedResponse.newBuilder();
    int size = names.size();
    for (int i = 0; i < size; i++) {
      if (!context.getMatrixMetaManager().isCreated(names.get(i))) {
        builder.setStatus(-1);
        return builder.build();
      }
    }

    return builder.setStatus(0).build();
  }

  /**
   * Check save request is complete
   */
  @Override
  public CheckModelSavedResponse checkModelSaved(RpcController controller,
      CheckModelSavedRequest request) throws ServiceException {
    LOG.info("check model saved=" + request);
    ModelSaveResult result = context.getModelSaver().getModelSaveResult(request.getRequestId());
    if (result == null) {
      throw new ServiceException("can not find save request " + request.getRequestId());
    } else {
      CheckModelSavedResponse.Builder builder = CheckModelSavedResponse.newBuilder();
      builder.setStatus(result.getState().getStateId());
      if (result.getMessage() != null) {
        builder.setLog(result.getMessage());
      }
      return builder.build();
    }
  }

  @Override
  public CheckModelLoadedResponse checkModelLoaded(RpcController controller,
      CheckModelLoadedRequest request) throws ServiceException {
    LOG.info("check model loaded=" + request);
    ModelLoadResult result = context.getModelLoader().getModelLoadResult(request.getRequestId());
    if (result == null) {
      throw new ServiceException("can not find load request " + request.getRequestId());
    } else {
      CheckModelLoadedResponse.Builder builder = CheckModelLoadedResponse.newBuilder();
      builder.setStatus(result.getState().getStateId());
      if (result.getMessage() != null) {
        builder.setLog(result.getMessage());
      }
      return builder.build();
    }
  }

  /**
   * Set parameters.
   *
   * @param controller rpc controller of protobuf
   * @param request check request that contains parameter keys and values
   */
  @Override
  public SetParamsResponse setParams(RpcController controller, SetParamsRequest request)
      throws ServiceException {
    List<Pair> kvs = request.getKvsList();
    int size = kvs.size();
    for (int i = 0; i < size; i++) {
      context.getConf().set(kvs.get(i).getKey(), kvs.get(i).getValue());
    }

    return SetParamsResponse.newBuilder().build();
  }

  @Override
  public GetClientIdResponse getClientId(RpcController controller, GetClientIdRequest request)
      throws ServiceException {
    return GetClientIdResponse.newBuilder().setClientId(context.getClientManager().getId()).build();
  }

  @Override
  public KeepAliveResponse keepAlive(RpcController controller, KeepAliveRequest request)
      throws ServiceException {
    LOG.info("keep alive " + request);
    context.getClientManager().alive(request.getClientId());
    return KeepAliveResponse.getDefaultInstance();
  }

  @Override
  public ClientRegisterResponse clientRegister(RpcController controller,
      ClientRegisterRequest request) throws ServiceException {
    context.getClientManager().register(request.getClientId());
    return ClientRegisterResponse.getDefaultInstance();
  }

  @Override
  public KillPSResponse killPS(RpcController controller, KillPSRequest request)
      throws ServiceException {
    PSAttemptId psAttemptId = new PSAttemptId(new ParameterServerId(request.getPsId()),
        request.getAttemptIndex());
    //LOG.info("error happened in psAttempt " + psAttemptId + " error msg=" + request.getMsg());

    //remove this parameter server attempt from monitor set
    context.getParameterServerManager().unRegister(psAttemptId);

    context.getEventHandler()
        .handle(new PSAttemptDiagnosticsUpdateEvent("kill by client", psAttemptId));

    context.getEventHandler()
        .handle(new PSAttemptEvent(PSAttemptEventType.PA_FAILMSG, psAttemptId));
    return KillPSResponse.getDefaultInstance();
  }
}
