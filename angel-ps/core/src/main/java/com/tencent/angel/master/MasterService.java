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

package com.tencent.angel.master;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.location.LocationManager;
import com.tencent.angel.ipc.MLRPC;
import com.tencent.angel.ipc.RpcServer;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.AppEvent;
import com.tencent.angel.master.app.AppEventType;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.matrixmeta.AMMatrixMetaManager;
import com.tencent.angel.master.metrics.MetricsEvent;
import com.tencent.angel.master.metrics.MetricsEventType;
import com.tencent.angel.master.metrics.MetricsUpdateEvent;
import com.tencent.angel.master.ps.CommitEvent;
import com.tencent.angel.master.ps.attempt.*;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.attempt.*;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupState;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixReport;
import com.tencent.angel.ml.matrix.transport.PSLocation;
import com.tencent.angel.ml.metric.Metric;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.MLProtos.*;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.*;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.recovery.ha.RecoverPartKey;
import com.tencent.angel.utils.KryoUtils;
import com.tencent.angel.utils.NetUtils;
import com.tencent.angel.utils.StringUtils;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import scala.Int;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * the RPC server for angel application master. it respond to requests from clients, worker and ps
 */
public class MasterService extends AbstractService implements MasterProtocol {
  private static final Log LOG = LogFactory.getLog(MasterService.class);
  private final AMContext context;
  /**RPC server*/
  private RpcServer rpcServer;

  /**heartbeat timeout check thread*/
  private Thread timeOutChecker;

  private final AtomicBoolean stopped;

  /**received matrix meta from client*/
  private final List<MatrixMeta> matrics;

  /**host and port of the RPC server*/
  private volatile Location location;

  /** Yarn web port */
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
    String nmWebAddr = conf.get(YarnConfiguration.NM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_ADDRESS);
    String [] addrItems = nmWebAddr.split(":");
    if(addrItems.length == 2) {
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
   * @param controller rpc controller of protobuf
   * @param request heartbeat request
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSReportResponse psReport(RpcController controller, PSReportRequest request)
      throws ServiceException {
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
      //refresh last heartbeat timestamp
      context.getParameterServerManager().alive(psAttemptId);

      //send a state update event to the specific PSAttempt
      context.getEventHandler().handle(new PSAttemptStateUpdateEvent(psAttemptId, paramsMap));

      //check if parameter server can commit now.
      if (context.getParameterServerManager().psCanCommit()) {
        List<Integer> ids = context.getParameterServerManager().getNeedCommitMatrixIds();
        LOG.info("notify ps" + psAttemptId + " to commit now! commit matrices:" + StringUtils.joinInts(",", ids));
        resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_COMMIT);

        NeedSaveMatrixProto.Builder saveBuilder = NeedSaveMatrixProto.newBuilder();
        for(int matrixId : ids) {
          resBuilder.addNeedSaveMatrices(saveBuilder.setMatrixId(matrixId).addAllPartIds(
            context.getMatrixMetaManager().getMasterPartsInPS(matrixId, psAttemptId.getPsId())).build());
          saveBuilder.clear();
        }
      } else {
        resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_OK);
      }
    }

    //check matrix metadata inconsistencies between master and parameter server.
    //if a matrix exists on the Master and does not exist on ps, then it is necessary to notify ps to establish the matrix
    //if a matrix exists on the ps and does not exist on master, then it is necessary to notify ps to remove the matrix
    List<MatrixReportProto> matrixReportsProto = request.getMatrixReportsList();
    List<Integer> needReleaseMatrices = new ArrayList<>();
    List<MatrixMeta> needCreateMatrices = new ArrayList<>();
    List<RecoverPartKey> needRecoverParts = new ArrayList<>();

    List<MatrixReport> matrixReports = ProtobufUtil.convertToMatrixReports(matrixReportsProto);
    context.getMatrixMetaManager().syncMatrixInfos(
      matrixReports, needCreateMatrices, needReleaseMatrices, needRecoverParts, psAttemptId.getPsId());

    size = needCreateMatrices.size();
    for (int i = 0; i < size; i++) {
      resBuilder.addNeedCreateMatrices(ProtobufUtil.convertToMatrixMetaProto(needCreateMatrices.get(i)));
    }

    size = needReleaseMatrices.size();
    for (int i = 0; i < size; i++) {
      resBuilder.addNeedReleaseMatrixIds(needReleaseMatrices.get(i));
    }

    size = needRecoverParts.size();
    for (int i = 0; i < size; i++) {
      resBuilder.addNeedRecoverParts(ProtobufUtil.convert(needRecoverParts.get(i)));
    }

    return resBuilder.build();
  }

  @Override
  public PSAgentMasterServiceProtos.FetchMinClockResponse fetchMinClock(RpcController controller, PSAgentMasterServiceProtos.FetchMinClockRequest request) {
    return PSAgentMasterServiceProtos.FetchMinClockResponse.newBuilder().setMinClock(10).build();
  }

  /**
   * response for parameter server register.
   * @param controller rpc controller of protobuf
   * @param request register request
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSRegisterResponse psRegister(RpcController controller, PSRegisterRequest request)
      throws ServiceException {
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
      context.getEventHandler()
          .handle(
              new PSAttemptRegisterEvent(psAttemptId, new Location(request.getLocation().getIp(), request.getLocation()
                  .getPort())));
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

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    //choose a unused port
    int servicePort = NetUtils.chooseAListenPort(conf);
    String ip = NetUtils.getRealLocalIP();
    LOG.info("listen ip:" + ip + ", port:" + servicePort);

    location = new Location(ip, servicePort);
    //start RPC server
    this.rpcServer =
        MLRPC.getServer(MasterService.class, this, new Class<?>[] {MasterProtocol.class}, ip,
            servicePort, conf);
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
   * @param controller rpc controller of protobuf
   * @param request request
   * @throws ServiceException
   */
  @Override
  public GetJobReportResponse getJobReport(RpcController controller, GetJobReportRequest request)
      throws ServiceException {
    GetJobReportResponse response = context.getApp().getJobReportResponse();
    return response;
  }

  /**
   * Get worker log url
   * @param controller rpc controller
   * @param request rpc request contains worker id
   * @return worker log url
   * @throws ServiceException  worker does not exist
   */
  @Override
  public GetWorkerLogDirResponse getWorkerLogDir(RpcController controller,
                                                 GetWorkerLogDirRequest request) throws ServiceException {
    WorkerId workerId = ProtobufUtil.convertToId(request.getWorkerId());
    AMWorker worker = context.getWorkerManager().getWorker(workerId);
    if(worker == null){
      throw new ServiceException("can not find worker " + workerId);
    }

    WorkerAttempt workerAttempt = worker.getRunningAttempt();
    if(workerAttempt == null) {
      return GetWorkerLogDirResponse.newBuilder().setLogDir("").build();
    }

    Location loc = workerAttempt.getLocation();
    Container container = workerAttempt.getContainer();
    if(loc == null || container == null){
      return GetWorkerLogDirResponse.newBuilder().setLogDir("").build();
    }

    return GetWorkerLogDirResponse.newBuilder().setLogDir(
            "http://" + loc.getIp() + ":" + yarnNMWebPort + "/node/containerlogs/"
                    + container.getId() + "/angel/syslog/?start=0").build();
  }

  /**
   * set matrix meta
   * @param controller rpc controller of protobuf
   * @param request matrix meta
   * @throws ServiceException
   */
  @Override
  public CreateMatricesResponse createMatrices(RpcController controller,
      CreateMatricesRequest request) throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive create matrix request. request=" + request);
    }

    try {
      context.getMatrixMetaManager().createMatrices(ProtobufUtil.convertToMatrixContexts(request.getMatricesList()));
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
   * @param controller
   * @param request
   * @return
   * @throws ServiceException
   */
  @Override
  public GetMatricesResponse getMatrices(RpcController controller, GetMatricesRequest request)
    throws ServiceException {
    GetMatricesResponse.Builder builder = GetMatricesResponse.newBuilder();
    AMMatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();

    List<String> matrixNames = request.getMatrixNamesList();
    int size = matrixNames.size();
    for(int i = 0; i < size; i++) {
      MatrixMeta matrixMeta = matrixMetaManager.getMatrix(matrixNames.get(i));
      if(matrixMeta == null) {
        throw new ServiceException("Can not find matrix " + matrixNames.get(i));
      }
      builder.addMatrixMetas(ProtobufUtil.convertToMatrixMetaProto(matrixMeta));
    }
    return builder.build();
  }

  /**
   * Release matrices
   * @param controller
   * @param request
   * @return
   * @throws ServiceException
   */
  @Override public ReleaseMatricesResponse releaseMatrices(RpcController controller,
    ReleaseMatricesRequest request) throws ServiceException {
    AMMatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();
    List<String> matrixNames = request.getMatrixNamesList();

    int size = matrixNames.size();
    for(int i = 0; i < size; i++) {
      matrixMetaManager.releaseMatrix(matrixNames.get(i));
    }
    return ReleaseMatricesResponse.newBuilder().build();
  }

  public InetSocketAddress getRPCListenAddr() {
    return rpcServer.getListenerAddress();
  }

  /**
   * notify a parameter server run over successfully
   * @param controller rpc controller of protobuf
   * @param request parameter server attempt id
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSDoneResponse psDone(RpcController controller, PSDoneRequest request)
      throws ServiceException {
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
   * @param controller rpc controller of protobuf
   * @param request contains parameter server id and error message
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSErrorResponse psError(RpcController controller, PSErrorRequest request)
      throws ServiceException {
    PSAttemptId psAttemptId = ProtobufUtil.convertToId(request.getPsAttemptId());
    LOG.info("error happened in psAttempt " + psAttemptId + " error msg=" + request.getMsg());

    //remove this parameter server attempt from monitor set
    context.getParameterServerManager().unRegister(psAttemptId);

    context.getEventHandler().handle(
        new PSAttemptDiagnosticsUpdateEvent(request.getMsg(), psAttemptId));

    context.getEventHandler()
        .handle(new PSAttemptEvent(PSAttemptEventType.PA_FAILMSG, psAttemptId));

    return PSErrorResponse.newBuilder().build();
  }

  /**
   * get all matrix meta
   * @param controller rpc controller of protobuf
   * @param request
   * @throws ServiceException
   */
  @Override
  public GetAllMatrixMetaResponse getAllMatrixMeta(RpcController controller,
      GetAllMatrixMetaRequest request) throws ServiceException {

    GetAllMatrixMetaResponse.Builder resBuilder = GetAllMatrixMetaResponse.newBuilder();
    Map<Integer, MatrixMeta> matrixIdToMetaMap = context.getMatrixMetaManager().getMatrixMetas();

    for(Entry<Integer, MatrixMeta> metaEntry : matrixIdToMetaMap.entrySet()) {
      resBuilder.addMatrixMetas(ProtobufUtil.convertToMatrixMetaProto(metaEntry.getValue()));
    }
    return resBuilder.build();
  }

  /**
   * get all parameter server locations.
   * @param controller rpc controller of protobuf
   * @param request
   * @throws ServiceException
   */
  @Override
  public GetAllPSLocationResponse getAllPSLocation(RpcController controller,
      GetAllPSLocationRequest request) {
    GetAllPSLocationResponse.Builder resBuilder = GetAllPSLocationResponse.newBuilder();
    LocationManager locationManager = context.getLocationManager();
    ParameterServerId [] psIds = locationManager.getPsIds();
    for(int i = 0; i < psIds.length; i++) {
      resBuilder.addPsLocations(ProtobufUtil.convertToPSLocProto(psIds[i], locationManager.getPsLocation(psIds[i])));
    }
    return resBuilder.build();
  }

  /**
   * get a specific parameter server location.
   * @param controller rpc controller of protobuf
   * @param request parameter server id
   * @throws ServiceException
   */
  @Override
  public GetPSLocationReponse getPSLocation(RpcController controller, GetPSLocationRequest request)
      throws ServiceException {
    GetPSLocationReponse.Builder resBuilder = GetPSLocationReponse.newBuilder();
    ParameterServerId psId = ProtobufUtil.convertToId(request.getPsId());

    Location psLocation = context.getLocationManager().getPsLocation(psId);
    if (psLocation == null) {
      resBuilder.setPsLocation(PSLocationProto.newBuilder().setPsId(request.getPsId()).setPsStatus(PSStatus.PS_NOTREADY).build());
    } else {
      resBuilder.setPsLocation(ProtobufUtil.convertToPSLocProto(psId, psLocation));
    }
    return resBuilder.build();
  }

  /**
   * Get locations for a partition
   * @param controller
   * @param request
   * @return
   * @throws ServiceException
   */
  @Override public GetPartLocationResponse getPartLocation(RpcController controller,
    GetPartLocationRequest request) throws ServiceException {
    GetPartLocationResponse.Builder builder = GetPartLocationResponse.newBuilder();
    List<ParameterServerId> psIds = context.getMatrixMetaManager().getPss(request.getMatrixId(), request.getPartId());

    if(psIds != null) {
      int size = psIds.size();
      for(int i = 0; i < size; i++) {
        Location psLocation = context.getLocationManager().getPsLocation(psIds.get(i));
        if (psLocation == null) {
          builder.addLocations((PSLocationProto.newBuilder().setPsId(
            ProtobufUtil.convertToIdProto(psIds.get(i))).setPsStatus(PSStatus.PS_NOTREADY).build()));
        } else {
          builder.addLocations(ProtobufUtil.convertToPSLocProto(psIds.get(i), psLocation));
        }
      }
    }

    return builder.build();
  }

  /**
   * Get iteration now
   * @param controller
   * @param request
   * @return
   * @throws ServiceException
   */
  @Override
  public GetIterationResponse getIteration(RpcController controller, GetIterationRequest request)
    throws ServiceException {
    int curIteration = 0;
    if(context.getAlgoMetricsService() != null) {
      curIteration = context.getAlgoMetricsService().getCurrentIter();
    }
    return GetIterationResponse.newBuilder().setIteration(curIteration).build();
  }

  @Override public GetPSMatricesResponse getPSMatricesMeta(RpcController controller,
    GetPSMatricesMetaRequest request) throws ServiceException {
    Map<Integer, MatrixMeta> matrixIdToMetaMap = context.getMatrixMetaManager().getMatrixPartitions(ProtobufUtil.convertToId(request.getPsId()));
    GetPSMatricesResponse.Builder builder = GetPSMatricesResponse.newBuilder();
    if(matrixIdToMetaMap != null && !matrixIdToMetaMap.isEmpty()) {
      for(MatrixMeta meta : matrixIdToMetaMap.values()) {
        builder.addMatricesMeta(ProtobufUtil.convertToMatrixMetaProto(meta));
      }
    }
    return builder.build();
  }

  /**
   * Get the stored pss of a matrix partition
   * @param controller
   * @param request
   * @return
   * @throws ServiceException
   */
  @Override
  public GetStoredPssResponse getStoredPss(RpcController controller, GetStoredPssRequest request)
    throws ServiceException {
    GetStoredPssResponse.Builder builder = GetStoredPssResponse.newBuilder();
    List<ParameterServerId> psIds = context.getMatrixMetaManager().getPss(request.getMatrixId(), request.getMatrixId());

    if(psIds != null) {
      int size = psIds.size();
      for(int i = 0; i < size; i++) {
        builder.addPsIds(ProtobufUtil.convertToIdProto(psIds.get(i)));
      }
    }
    return builder.build();
  }

  /**
   * Get a new psagent id
   * @param controller
   * @param request
   * @return
   * @throws ServiceException
   */
  @Override
  public GetPSAgentIdResponse getPSAgentId(RpcController controller, GetPSAgentIdRequest request)
    throws ServiceException {
    return GetPSAgentIdResponse.newBuilder().setPsAgentId(context.getPSAgentManager().getId()).build();
  }

  /**
   * Check PS exited or not
   * @param controller
   * @param request
   * @return
   * @throws ServiceException
   */
  @Override
  public CheckPSExitResponse checkPSExited(RpcController controller, CheckPSExitRequest request)
    throws ServiceException {
    if(context.getParameterServerManager().checkFailed(ProtobufUtil.convert(request.getPsLoc()))) {
      return CheckPSExitResponse.newBuilder().setExited(1).build();
    } else {
      return CheckPSExitResponse.newBuilder().setExited(0).build();
    }
  }


  /**
   * response for psagent heartbeat.
   *
   * @param controller rpc controller of protobuf
   * @param request
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSAgentReportResponse psAgentReport(RpcController controller, PSAgentReportRequest request)
      throws ServiceException {
    return PSAgentReportResponse.newBuilder().build();
  }

  /**
   * response for psagent heartbeat.
   *
   * @param controller rpc controller of protobuf
   * @param request contains psagent attempt id
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSAgentRegisterResponse psAgentRegister(RpcController controller,
      PSAgentRegisterRequest request) throws ServiceException {
    LOG.info("PSAgent register:"  + request);
    return PSAgentRegisterResponse.newBuilder().setCommand(PSAgentCommandProto.PSAGENT_SUCCESS).build();
  }

  /**
   * psagent run over successfully
   *
   * @param controller rpc controller of protobuf
   * @param request contains psagent attempt id
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSAgentDoneResponse psAgentDone(RpcController controller, PSAgentDoneRequest request)
      throws ServiceException {
    PSAgentDoneResponse.Builder resBuilder = PSAgentDoneResponse.newBuilder();
    return resBuilder.build();
  }

  /**
   * psagent run falied
   *
   * @param controller rpc controller of protobuf
   * @param request contains psagent attempt id, error message
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public PSAgentErrorResponse psAgentError(RpcController controller, PSAgentErrorRequest request)
      throws ServiceException {
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
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public WorkerReportResponse workerReport(RpcController controller, WorkerReportRequest request)
      throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive worker report, request=" + request);
    }

    WorkerAttemptId workerAttemptId = ProtobufUtil.convertToId(request.getWorkerAttemptId());
    if (!context.getWorkerManager().isAlive(workerAttemptId)) {
      LOG.error("worker attempt " + workerAttemptId + " is not in running worker attempt set now, shutdown it");
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
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public WorkerRegisterResponse workerRegister(RpcController controller,
      WorkerRegisterRequest request) throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive worker register, request=" + request);
    }
    WorkerRegisterResponse.Builder registerResponseBuilder = WorkerRegisterResponse.newBuilder();
    WorkerAttemptId workerAttemptId = ProtobufUtil.convertToId(request.getWorkerAttemptId());

    //if worker attempt id is not in monitor set, we should shutdown it
    if (!context.getWorkerManager().isAlive(workerAttemptId)) {
      LOG.error("worker attempt " + workerAttemptId + " is not in running worker attempt set now, shutdown it");
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
   * @throws ServiceException
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

    if (group == null || group.getState() == AMWorkerGroupState.NEW || group.getState() == AMWorkerGroupState.INITED) {
      //if this worker group does not initialized, just return WORKERGROUP_NOTREADY
      return GetWorkerGroupMetaInfoResponse
          .newBuilder()
          .setWorkerGroupStatus(
              GetWorkerGroupMetaInfoResponse.WorkerGroupStatus.WORKERGROUP_NOTREADY).build();
    } else if (group.getState() == AMWorkerGroupState.FAILED
        || group.getState() == AMWorkerGroupState.KILLED
        || group.getState() == AMWorkerGroupState.SUCCESS) {
      //if this worker group run over, just return WORKERGROUP_EXITED
      return GetWorkerGroupMetaInfoResponse
          .newBuilder()
          .setWorkerGroupStatus(GetWorkerGroupMetaInfoResponse.WorkerGroupStatus.WORKERGROUP_EXITED)
          .build();
    } else {
      //if this worker group is running now, return tasks, workers, data splits for it
      try {
        return ProtobufUtil.buildGetWorkerGroupMetaResponse(group, context.getDataSpliter()
            .getSplits(group.getSplitIndex()), context.getConf());
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
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public WorkerDoneResponse workerDone(RpcController controller, WorkerDoneRequest request)
      throws ServiceException {
    WorkerAttemptId workerAttemptId = ProtobufUtil.convertToId(request.getWorkerAttemptId());
    LOG.info("worker attempt " + workerAttemptId + " is done");
    WorkerDoneResponse.Builder resBuilder = WorkerDoneResponse.newBuilder();

    //if worker attempt id is not in monitor set, we should shutdown it
    if (!context.getWorkerManager().isAlive(workerAttemptId)) {
      resBuilder.setCommand(WorkerCommandProto.W_SHUTDOWN);
    } else {
      context.getWorkerManager().unRegister(workerAttemptId);
      resBuilder.setCommand(WorkerCommandProto.W_SUCCESS);
      context.getEventHandler().handle(new WorkerAttemptEvent(WorkerAttemptEventType.DONE, workerAttemptId));
    }

    return resBuilder.build();
  }

  /**
   * worker run failed
   *
   * @param controller rpc controller of protobuf
   * @param request contains worker attempt id, error message
   * @throws ServiceException
   */
  @SuppressWarnings("unchecked")
  @Override
  public WorkerErrorResponse workerError(RpcController controller, WorkerErrorRequest request)
      throws ServiceException {
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
      context.getEventHandler().handle(new WorkerAttemptEvent(WorkerAttemptEventType.ERROR, workerAttemptId));
      resBuilder.setCommand(WorkerCommandProto.W_SUCCESS);
    }

    return resBuilder.build();
  }

  /**
   * Get success Worker group number
   * @param controller rpc controller of protobuf
   * @param request empty
   * @return success Worker group number
   * @throws ServiceException
   */
  @Override
  public GetWorkerGroupSuccessNumResponse getWorkerGroupSuccessNum(RpcController controller,
    GetWorkerGroupSuccessNumRequest request) throws ServiceException {
    return GetWorkerGroupSuccessNumResponse.newBuilder().setSuccessNum(
      context.getWorkerManager().getSuccessWorkerGroupNum()).build();
  }

  public List<MatrixMeta> getMatrics() {
    return matrics;
  }

  /**
   * task update the clock for a matrix
   * @param controller rpc controller of protobuf
   * @param request contains task id, matrix id and clock value
   * @throws ServiceException
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
    if(task == null) {
      task = new AMTask(taskId, null);
      context.getTaskManager().putTask(taskId, task);
    }

    //update the clock for this matrix
    task.clock(request.getMatrixClock().getMatrixId(), request.getMatrixClock().getClock());
    return TaskClockResponse.newBuilder().build();
  }

  /**
   * task update iteration number
   * @param controller rpc controller of protobuf
   * @param request contains task id, iteration number
   * @throws ServiceException
   */
  @Override
  public TaskIterationResponse taskIteration(RpcController controller, TaskIterationRequest request)
      throws ServiceException {
    LOG.debug("task iteration, " + request);
    TaskId taskId = ProtobufUtil.convertToId(request.getTaskId());

    //get Task meta from task manager, if can not find, just new a AMTask object and put it to task manager
    //in ANGEL_PS mode, task id may can not know advance
    AMTask task = context.getTaskManager().getTask(taskId);
    if(task == null) {
      task = new AMTask(taskId, null);
      context.getTaskManager().putTask(taskId, task);
    }

    //update task iteration
    task.iteration(request.getIteration());
    context.getEventHandler().handle(new MetricsEvent(MetricsEventType.TASK_ITERATION_UPDATE));
    return TaskIterationResponse.newBuilder().build();
  }

  @Override public PSAgentMasterServiceProtos.TaskCountersUpdateResponse taskCountersUpdate(
    RpcController controller, PSAgentMasterServiceProtos.TaskCounterUpdateRequest request)
    throws ServiceException {
    AMTask task = context.getTaskManager().getTask(ProtobufUtil.convertToId(request.getTaskId()));
    if(task != null) {
      task.updateCounters(request.getCountersList());
    }
    return PSAgentMasterServiceProtos.TaskCountersUpdateResponse.newBuilder().build();
  }

  /**
   * Set algorithm metrics
   * @param controller rpc controller of protobuf
   * @param request request contains algorithm metrics of a task
   * @return
   * @throws ServiceException
   */
  @Override
  public PSAgentMasterServiceProtos.SetAlgoMetricsResponse setAlgoMetrics(RpcController controller,
    PSAgentMasterServiceProtos.SetAlgoMetricsRequest request) throws ServiceException {
    List<PSAgentMasterServiceProtos.AlgoMetric> metrics = request.getAlgoMetricsList();
    int size = metrics.size();
    Map<String, Metric> nameToMetricMap = new LinkedHashMap<>(size);
    for(int i = 0; i < size; i++) {
      nameToMetricMap.put(metrics.get(i).getName(), KryoUtils.deserializeAlgoMetric(metrics.get(i).getSerializedMetric().toByteArray()));
    }
    context.getEventHandler().handle(new MetricsUpdateEvent(nameToMetricMap));
    return PSAgentMasterServiceProtos.SetAlgoMetricsResponse.newBuilder().build();
  }

  @Override public PSFailedReportResponse psFailedReport(RpcController controller,
    PSFailedReportRequest request) throws ServiceException {
    LOG.info("Receive client ps failed report " + request);
    PSLocation psLoc = ProtobufUtil.convert(request.getPsLoc());
    context.getParameterServerManager().psFailedReport(psLoc);
    return PSFailedReportResponse.newBuilder().build();
  }

  /**
   * get clock of all matrices for all task
   * @param controller rpc controller of protobuf
   * @param request contains task id
   * @throws ServiceException
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
    for(AMTask task:tasks){
      taskBuilder.setTaskId(ProtobufUtil.convertToIdProto(task.getTaskId()));
      matrixClocks = task.getMatrixClocks();
      for(it.unimi.dsi.fastutil.ints.Int2IntMap.Entry entry:matrixClocks.int2IntEntrySet()) {
        taskBuilder.addMatrixClocks(matrixClockBuilder.setMatrixId(entry.getIntKey()).setClock(entry.getIntValue()).build());
      }
      builder.addTaskMatrixClocks(taskBuilder.build());
      taskBuilder.clear();
    }

    return builder.build();
  }

  /**
   * use to check a RPC connection to master is established
   * @param controller rpc controller of protobuf
   * @param request a empty request
   * @throws ServiceException
   */
  @Override
  public PingResponse ping(RpcController controller, PingRequest request) throws ServiceException {
    return PingResponse.newBuilder().build();
  }

  public Location getLocation() {
    return location;
  }


  /**
   * Start executing.
   * @param controller rpc controller of protobuf
   * @param request start request
   * @throws ServiceException
   */
  @Override
  public StartResponse start(RpcController controller, StartRequest request)
      throws ServiceException {
    LOG.info("start to calculation");
    context.getApp().startExecute();
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
  public SaveResponse save(RpcController controller, SaveRequest request) throws ServiceException {
    List<String> needSaveMatrices = request.getMatrixNamesList();

    List<Integer> matrixIds = new ArrayList<Integer> (needSaveMatrices.size());
    AMMatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();
    int size = needSaveMatrices.size();
    for(int i = 0; i < size; i++) {
      MatrixMeta matrixMeta = matrixMetaManager.getMatrix(needSaveMatrices.get(i));
      if(matrixMeta == null) {
        throw new ServiceException("matrix " + needSaveMatrices.get(i) + " does not exist");
      }
      LOG.info("Need save matrix " + matrixMeta.getName());
      matrixIds.add(matrixMeta.getId());
    }

    context.getEventHandler().handle(new CommitEvent(matrixIds));
    context.getEventHandler().handle(new AppEvent(AppEventType.COMMIT));

    return SaveResponse.newBuilder().build();
  }

  /**
   * Set the application to a given finish state
   * @param controller rpc controller
   * @param request application finish state
   * @return response
   * @throws ServiceException
   */
  @Override
  public ClientMasterServiceProtos.StopResponse stop(RpcController controller,
    ClientMasterServiceProtos.StopRequest request) throws ServiceException {
    LOG.info("receive stop command from client, request=" + request);
    stop(request.getExitStatus());
    return ClientMasterServiceProtos.StopResponse.newBuilder().build();
  }

  public void stop(int exitStatus) {
    switch(exitStatus) {
      case 1:{
        context.getEventHandler().handle(new AppEvent(AppEventType.KILL));
        break;
      }
      case 2:{
        context.getEventHandler().handle(new InternalErrorEvent(context.getApplicationId(), "stop the application with failed status"));
        break;
      }
      default:{
        context.getEventHandler().handle(new AppEvent(AppEventType.SUCCESS));
      }
    }
  }

  /**
   * Check matrices are created successfully
   * @param controller rpc controller of protobuf
   * @param request check request that contains matrix names
   */
  @Override
  public CheckMatricesCreatedResponse checkMatricesCreated(RpcController controller,
      CheckMatricesCreatedRequest request) throws ServiceException {
    List<String> names = request.getMatrixNamesList();
    CheckMatricesCreatedResponse.Builder builder = CheckMatricesCreatedResponse.newBuilder();
    int size = names.size();
    for(int i = 0; i < size; i++) {
      if(!context.getMatrixMetaManager().isCreated(names.get(i))) {
        builder.setStatus(-1);
        return builder.build();
      }
    }

    return builder.setStatus(0).build();
  }
  /**
   * Set parameters.
   * @param controller rpc controller of protobuf
   * @param request check request that contains parameter keys and values
   */
  @Override
  public SetParamsResponse setParams(RpcController controller, SetParamsRequest request)
      throws ServiceException {
    List<Pair> kvs = request.getKvsList();
    int size = kvs.size();
    for(int i = 0; i < size; i++) {
      context.getConf().set(kvs.get(i).getKey(), kvs.get(i).getValue());
    }

    return SetParamsResponse.newBuilder().build();
  }

  @Override
  public GetClientIdResponse getClientId(RpcController controller, GetClientIdRequest request)
    throws ServiceException {
    return GetClientIdResponse.newBuilder().setClientId(context.getClientManager().getId()).build();
  }

  @Override public KeepAliveResponse keepAlive(RpcController controller, KeepAliveRequest request)
    throws ServiceException {
    context.getClientManager().alive(request.getClientId());
    return KeepAliveResponse.getDefaultInstance();
  }

  @Override public ClientRegisterResponse clientRegister(RpcController controller,
    ClientRegisterRequest request) throws ServiceException {
    context.getClientManager().register(request.getClientId());
    return ClientRegisterResponse.getDefaultInstance();
  }

}
