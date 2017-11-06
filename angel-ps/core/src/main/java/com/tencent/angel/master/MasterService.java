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
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ipc.MLRPC;
import com.tencent.angel.ipc.RpcServer;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.AppEvent;
import com.tencent.angel.master.app.AppEventType;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.metrics.MetricsEvent;
import com.tencent.angel.master.metrics.MetricsEventType;
import com.tencent.angel.master.metrics.MetricsUpdateEvent;
import com.tencent.angel.master.ps.CommitEvent;
import com.tencent.angel.master.ps.attempt.*;
import com.tencent.angel.master.psagent.*;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.attempt.*;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupState;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.metric.Metric;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.MLProtos.*;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.*;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.PSAgentAttemptId;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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

  /**worker attempt id to last heartbeat timestamp map*/
  private final ConcurrentHashMap<WorkerAttemptId, Long> workerLastHeartbeatTS;

  /**parameter server attempt id to last heartbeat timestamp map*/
  private final ConcurrentHashMap<PSAttemptId, Long> psLastHeartbeatTS;

  /**psagent attempt id to last heartbeat timestamp map*/
  private final ConcurrentHashMap<PSAgentAttemptId, Long> psAgentLastHeartbeatTS;

  /**psagent heartbeat timeout value in millisecond*/
  private final long psAgentTimeOutMS;

  /**parameter server heartbeat timeout value in millisecond*/
  private final long psTimeOutMS;

  /**worker timeout value in millisecond*/
  private final long workerTimeOutMS;

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
    workerLastHeartbeatTS = new ConcurrentHashMap<WorkerAttemptId, Long>();
    psAgentLastHeartbeatTS = new ConcurrentHashMap<PSAgentAttemptId, Long>();
    psLastHeartbeatTS = new ConcurrentHashMap<PSAttemptId, Long>();
    matrics = new ArrayList<MatrixMeta>();

    Configuration conf = context.getConf();
    psAgentTimeOutMS =
        conf.getLong(AngelConf.ANGEL_PSAGENT_HEARTBEAT_TIMEOUT_MS,
            AngelConf.DEFAULT_ANGEL_PSAGENT_HEARTBEAT_TIMEOUT_MS);

    psTimeOutMS =
        conf.getLong(AngelConf.ANGEL_PS_HEARTBEAT_TIMEOUT_MS,
            AngelConf.DEFAULT_ANGEL_PS_HEARTBEAT_TIMEOUT_MS);
    workerTimeOutMS =
        conf.getLong(AngelConf.ANGEL_WORKER_HEARTBEAT_TIMEOUT_MS,
            AngelConf.DEFAULT_ANGEL_WORKER_HEARTBEAT_TIMEOUT_MS);

    yarnNMWebPort = getYarnNMWebPort(conf);

    LOG.debug("psAgentTimeOutMS:" + psAgentTimeOutMS);
    LOG.debug("psTimeOutMS:" + psTimeOutMS);
    LOG.debug("workerTimeOutMS:" + workerTimeOutMS);
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

  public void registerPSAttemptId(PSAttemptId psAttemptId) {
    LOG.info(psAttemptId + " is registered in monitor!");
    psLastHeartbeatTS.put(psAttemptId, System.currentTimeMillis());
  }

  public void unRegisterPSAttemptId(PSAttemptId psAttemptId) {
    LOG.info(psAttemptId + " is finished,  delete it in monitor!");
    psLastHeartbeatTS.remove(psAttemptId);
  }

  public void registerPSAgentAttemptId(PSAgentAttemptId attemptId) {
    LOG.info(attemptId + " is registered in monitor!");
    psAgentLastHeartbeatTS.put(attemptId, System.currentTimeMillis());
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
    if (!psLastHeartbeatTS.containsKey(psAttemptId)) {
      //if psAttemptId is not in monitor set, just return a PSCOMMAND_SHUTDOWN command.
      LOG.error("ps attempt " + psAttemptId + " is not in running ps attempt set");
      resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_SHUTDOWN);
    } else {
      //refresh last heartbeat timestamp
      psLastHeartbeatTS.put(psAttemptId, System.currentTimeMillis());
      //send a state update event to the specific PSAttempt
      context.getEventHandler().handle(new PSAttemptStateUpdateEvent(psAttemptId, paramsMap));

      //check if parameter server can commit now.
      if (context.getParameterServerManager().psCanCommit()) {
        List<Integer> ids = context.getParameterServerManager().getNeedCommitMatrixIds();
        LOG.info("notify ps" + psAttemptId + " to commit now! commit matrices:" + StringUtils.joinInts(",", ids));
        resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_COMMIT);
        resBuilder.addAllNeedCommitMatrixIds(ids);
      } else {
        resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_OK);
      }
    }

    //check matrix metadata inconsistencies between master and parameter server.
    //if a matrix exists on the Master and does not exist on ps, then it is necessary to notify ps to establish the matrix
    //if a matrix exists on the ps and does not exist on master, then it is necessary to notify ps to remove the matrix
    List<MatrixReport> matrixReports = request.getMatrixReportsList();
    List<Integer> needReleaseMatrixes = new ArrayList<Integer>();
    List<MatrixPartition> needCreateMatrixes = new ArrayList<MatrixPartition>();
    context.getMatrixMetaManager().syncMatrixInfos(matrixReports, needCreateMatrixes,
        needReleaseMatrixes, psAttemptId.getParameterServerId());

    size = needCreateMatrixes.size();
    for (int i = 0; i < size; i++) {
      resBuilder.addNeedCreateMatrixIds(needCreateMatrixes.get(i));
    }

    size = needReleaseMatrixes.size();
    for (int i = 0; i < size; i++) {
      resBuilder.addNeedReleaseMatrixIds(needReleaseMatrixes.get(i));
    }

    context.getMatrixMetaManager().psMatricesUpdate(psAttemptId.getParameterServerId(), matrixReports);

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
    if (!psLastHeartbeatTS.containsKey(psAttemptId)) {
      LOG.info(psAttemptId + " doesn't exists!");
      for (PSAttemptId id : psLastHeartbeatTS.keySet()) {
        LOG.info("contains psKey: " + id);
      }
      resBuilder.setPsCommand(PSCommandProto.PSCOMMAND_SHUTDOWN);
    } else {
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
    startCheckThread();
    super.serviceStart();
  }

  private void startCheckThread() {
    timeOutChecker = new Thread(new Runnable() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {
        Iterator<Map.Entry<PSAgentAttemptId, Long>> psAgentIt = null;
        Iterator<Map.Entry<PSAttemptId, Long>> psIt = null;
        Entry<PSAgentAttemptId, Long> psAgentEntry = null;
        Entry<PSAttemptId, Long> psEntry = null;
        Iterator<Map.Entry<WorkerAttemptId, Long>> workerIt = null;
        Entry<WorkerAttemptId, Long> workerEntry = null;
        long currentTs = 0;

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.warn(Thread.currentThread().getName() + " is interupted");
          }
          currentTs = System.currentTimeMillis();

          //check whether psagent heartbeat timeout
          psAgentIt = psAgentLastHeartbeatTS.entrySet().iterator();
          while (psAgentIt.hasNext()) {
            psAgentEntry = psAgentIt.next();
            if (currentTs - psAgentEntry.getValue() > psAgentTimeOutMS) {
              context.getEventHandler().handle(
                  new PSAgentAttemptEvent(PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG,
                      psAgentEntry.getKey()));
              context.getEventHandler().handle(
                  new PSAgentAttemptDiagnosticsUpdateEvent(psAgentEntry.getKey(),
                      "heartbeat timeout"));
              LOG.info("removing psagent: " + psAgentEntry.getKey());
              psAgentIt.remove();
            }
          }

          //check whether parameter server heartbeat timeout
          psIt = psLastHeartbeatTS.entrySet().iterator();
          while (psIt.hasNext()) {
            psEntry = psIt.next();
            if (currentTs - psEntry.getValue() > psTimeOutMS) {
              LOG.error(psEntry.getKey() + " heartbeat timeout!!!");
              context.getEventHandler().handle(
                  new PSAttemptDiagnosticsUpdateEvent("heartbeat timeout", psEntry.getKey()));

              context.getEventHandler().handle(
                  new PSAttemptEvent(PSAttemptEventType.PA_FAILMSG, psEntry.getKey()));
              psIt.remove();
            }
          }

          //check whether worker heartbeat timeout
          workerIt = workerLastHeartbeatTS.entrySet().iterator();
          while (workerIt.hasNext()) {
            workerEntry = workerIt.next();
            if (currentTs - workerEntry.getValue() > workerTimeOutMS) {
              LOG.error(workerEntry.getKey() + " heartbeat timeout!!!");
              context.getEventHandler().handle(
                  new WorkerAttemptDiagnosticsUpdateEvent(workerEntry.getKey(), "heartbeat timeout"));

              context.getEventHandler().handle(
                  new WorkerAttemptEvent(WorkerAttemptEventType.ERROR, workerEntry.getKey()));
              workerIt.remove();
            }
          }
        }
      }
    });
    timeOutChecker.setName("Heartbeat Timeout checker");
    timeOutChecker.start();
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
      LOG.debug("receive set matrix partitions request. request=" + request);
    }

    try {
      context.getMatrixMetaManager().addMatrices(request.getMatricesList());
    } catch (InvalidParameterException e1) {
      throw new ServiceException(e1);
    }

    try {
      context.getAppStateStorage().writeMatrixMeta(context.getMatrixMetaManager());
    } catch (Exception e) {
      LOG.error("write matrix meta to file failed.", e);
    }
    return CreateMatricesResponse.newBuilder().build();
  }

  public InetSocketAddress getRPCListenAddr() {
    return rpcServer.getListenerAddress();
  }

  /**
   * get partitions meta for a specific matrix and a specific parameter server
   * @param rpcControl rpc controller of protobuf
   * @param request matrix id and parameter server attempt id
   * @throws ServiceException
   */
  @Override
  public GetMatrixPartitionResponse getMatrixPartition(RpcController rpcControl,
      GetMatrixPartitionRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive getMatrixPartition request. request=" + request);
    }

    GetMatrixPartitionResponse.Builder resBuilder = GetMatrixPartitionResponse.newBuilder();
    PSAttemptId psAttemptId = ProtobufUtil.convertToId(request.getPsAttemptId());

    //find matrix partitions from master matrix meta manager for this parameter server
    List<MatrixPartition> matrixPartitions =
        context.getMatrixMetaManager().getMatrixPartitions(psAttemptId.getParameterServerId());

    if (matrixPartitions != null) {
      for (MatrixPartition matrixPart : matrixPartitions) {
        resBuilder.addMatrixPartitions(matrixPart);
      }
      resBuilder.setMatrixStatus(MatrixStatus.M_OK);
    } else {
      resBuilder.setMatrixStatus(MatrixStatus.M_NOT_READY);
    }
    return resBuilder.build();
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
    psLastHeartbeatTS.remove(psAttemptId);

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
    psLastHeartbeatTS.remove(psAttemptId);

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
  public GetAllMatrixInfoResponse getAllMatrixInfo(RpcController controller,
      GetAllMatrixInfoRequest request) throws ServiceException {

    GetAllMatrixInfoResponse.Builder resBuilder = GetAllMatrixInfoResponse.newBuilder();
    List<MatrixProto> matrixProtos = context.getMatrixMetaManager().getMatrixProtos();
    if (matrixProtos == null) {
      resBuilder.setMatrixStatus(MatrixStatus.M_NOT_READY);
    } else {
      resBuilder.setMatrixStatus(MatrixStatus.M_OK);
      for (MatrixProto matrixProto : matrixProtos) {
        resBuilder.addMatrixInfo(matrixProto);
      }
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
    List<PSLocation> psLocList = context.getLocationManager().getAllPSLocation();
    for (PSLocation psLoc : psLocList) {
      resBuilder.addPsLocations(psLoc);
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

    LocationProto psLocation = context.getLocationManager().getPSLocation(psId);
    if (psLocation == null) {
      resBuilder.setPsStatus(PSStatus.PS_NOTREADY);
    } else {
      resBuilder.setPsStatus(PSStatus.PS_OK);
      resBuilder.setLocation(psLocation);
    }
    return resBuilder.build();
  }

  /**
   * create a matrix
   *
   * @param controller rpc controller of protobuf
   * @param request matrix meta
   * @throws ServiceException
   */
  @Override
  public CreateMatrixResponse createMatrix(RpcController controller, CreateMatrixRequest request)
      throws ServiceException {
    LOG.info("create matrix " + request);
    CreateMatrixResponse.Builder builder = CreateMatrixResponse.newBuilder();
    int matrixId = -1;

    try {
      matrixId = context.getMatrixMetaManager().addMatrix(request.getMatrixProto());
    } catch (InvalidParameterException e) {
      throw new ServiceException(e);
    }

    try {
      context.getAppStateStorage().writeMatrixMeta(context.getMatrixMetaManager());
    } catch (Exception e) {
      LOG.error("write matrix meta to file failed.", e);
    }

    builder.setMatrixStatus(MatrixStatus.M_NOT_READY);
    return builder.setMatrixId(matrixId).build();
  }

  /**
   * get a specific matrix meta
   * @param controller rpc controller of protobuf
   * @param request matrix id
   * @throws ServiceException
   */
  @Override
  public GetMatrixInfoResponse getMatrixInfo(RpcController controller, GetMatrixInfoRequest request)
      throws ServiceException {
    GetMatrixInfoResponse.Builder builder = GetMatrixInfoResponse.newBuilder();
    MatrixProto matrix = context.getMatrixMetaManager().getMatrix(request.getMatrixId());
    if (matrix == null) {
      builder.setMatrixStatus(MatrixStatus.M_NOT_READY);
    } else {
      builder.setMatrixStatus(MatrixStatus.M_OK).setMatrixInfo(matrix);
    }

    return builder.build();
  }

  /**
   * release a matrix
   * @param controller rpc controller of protobuf
   * @param request matrix id
   * @throws ServiceException
   */
  @Override
  public ReleaseMatrixResponse releaseMatrix(RpcController controller, ReleaseMatrixRequest request)
      throws ServiceException {
    context.getMatrixMetaManager().releaseMatrix(request.getMatrixId());
    try {
      context.getAppStateStorage().writeMatrixMeta(context.getMatrixMetaManager());
    } catch (Exception e) {
      LOG.error("write matrix meta to file failed.", e);
    }
    return ReleaseMatrixResponse.newBuilder().build();
  }

  public void unRegisterPSAgentAttemptID(PSAgentAttemptId attemptId) {
    LOG.info(attemptId + " is unregistered in monitor!");
    psAgentLastHeartbeatTS.remove(attemptId);
  }

  /**
   * get all psagent locations.
   * @param controller rpc controller of protobuf
   * @param request
   * @throws ServiceException
   */
  @Override
  public GetAllPSAgentLocationResponse getAllPSAgentLocation(RpcController controller,
      GetAllPSAgentLocationRequest request) throws ServiceException {
    GetAllPSAgentLocationResponse.Builder resBuilder = GetAllPSAgentLocationResponse.newBuilder();
    List<PSAgentLocation> psLocList = context.getLocationManager().getAllPSAgentLocation();
    for (PSAgentLocation psLoc : psLocList) {
      resBuilder.addPsAgentLocations(psLoc);
    }
    return resBuilder.build();
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive ps agent state report, request=" + request);
    }
    PSAgentAttemptId psAgentAttemptId = ProtobufUtil.convertToId(request.getPsAgentAttemptId());

    if (!psAgentLastHeartbeatTS.containsKey(psAgentAttemptId)) {
      LOG.error("psagent attempt " + psAgentAttemptId + " is not in running worker attempt set now, shutdown it");
      return PSAgentReportResponse.newBuilder().setCommand(PSAgentCommandProto.PSAGENT_SHUTDOWN)
          .build();

    } else {
      context.getEventHandler().handle(
          new PSAgentAttemptStateUpdateEvent(psAgentAttemptId, request));
      psAgentLastHeartbeatTS.put(psAgentAttemptId, System.currentTimeMillis());
      return PSAgentReportResponse.newBuilder().setCommand(PSAgentCommandProto.PSAGENT_SUCCESS)
          .build();
    }
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("receive ps agent register, request=" + request);
    }

    PSAgentRegisterResponse.Builder registerResponseBuilder = PSAgentRegisterResponse.newBuilder();

    PSAgentAttemptId psAgentAttemptId = ProtobufUtil.convertToId(request.getPsAgentAttemptId());
    if (!psAgentLastHeartbeatTS.containsKey(psAgentAttemptId)) {
      LOG.error("psagent attempt " + psAgentAttemptId + " is not in running worker attempt set now, shutdown it");
      registerResponseBuilder.setCommand(PSAgentCommandProto.PSAGENT_SHUTDOWN);
    } else {
      registerPSAgentAttemptId(psAgentAttemptId);
      Location location =
          new Location(request.getLocation().getIp(), request.getLocation().getPort());
      context.getEventHandler().handle(new PSAgentRegisterEvent(psAgentAttemptId, location));
      registerResponseBuilder.setCommand(PSAgentCommandProto.PSAGENT_SUCCESS);
    }

    LOG.info("psagent " + psAgentAttemptId + " register finished!");
    return registerResponseBuilder.build();
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
    PSAgentAttemptId psAgentAttemptId = ProtobufUtil.convertToId(request.getPsAgentAttemptId());
    LOG.info("psagent " + psAgentAttemptId + " is done");
    PSAgentDoneResponse.Builder resBuilder = PSAgentDoneResponse.newBuilder();

    if (!psAgentLastHeartbeatTS.containsKey(psAgentAttemptId)) {
      LOG.error("psagent attempt " + psAgentAttemptId + " is not in running worker attempt set now, shutdown it");
      resBuilder.setCommand(PSAgentCommandProto.PSAGENT_SHUTDOWN);
    } else {
      psAgentLastHeartbeatTS.remove(psAgentAttemptId);
      resBuilder.setCommand(PSAgentCommandProto.PSAGENT_SUCCESS);
      context.getEventHandler()
          .handle(
              new PSAgentAttemptEvent(PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
                  psAgentAttemptId));
    }

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
    PSAgentAttemptId psAgentAttemptId = ProtobufUtil.convertToId(request.getPsAgentAttemptId());

    if (!psAgentLastHeartbeatTS.containsKey(psAgentAttemptId)) {
      LOG.error("psagent attempt " + psAgentAttemptId + " is not in running worker attempt set now, shutdown it");
      resBuilder.setCommand(PSAgentCommandProto.PSAGENT_SHUTDOWN);
    } else {
      psAgentLastHeartbeatTS.remove(psAgentAttemptId);
      LOG.error("error happened in psagent " + psAgentAttemptId + ", error msg:" + request.getMsg());
      context.getEventHandler().handle(
          new PSAgentAttemptDiagnosticsUpdateEvent(psAgentAttemptId, request.getMsg()));
      context.getEventHandler()
          .handle(
              new PSAgentAttemptEvent(PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG,
                  psAgentAttemptId));
      resBuilder.setCommand(PSAgentCommandProto.PSAGENT_SUCCESS);
    }
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
    if (!workerLastHeartbeatTS.containsKey(workerAttemptId)) {
      LOG.error("worker attempt " + workerAttemptId + " is not in running worker attempt set now, shutdown it");
      return WorkerReportResponse.newBuilder().setCommand(WorkerCommandProto.W_SHUTDOWN).build();
    } else {
      context.getEventHandler().handle(new WorkerAttemptStateUpdateEvent(workerAttemptId, request));
      workerLastHeartbeatTS.put(workerAttemptId, System.currentTimeMillis());
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
    if (!workerLastHeartbeatTS.containsKey(workerAttemptId)) {
      LOG.error("worker attempt " + workerAttemptId + " is not in running worker attempt set now, shutdown it");
      registerResponseBuilder.setCommand(WorkerCommandProto.W_SHUTDOWN);
    } else {
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
    if (!workerLastHeartbeatTS.containsKey(workerAttemptId)) {
      resBuilder.setCommand(WorkerCommandProto.W_SHUTDOWN);
    } else {
      workerLastHeartbeatTS.remove(workerAttemptId);
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
    if (!workerLastHeartbeatTS.containsKey(workerAttemptId)) {
      resBuilder.setCommand(WorkerCommandProto.W_SHUTDOWN);
    } else {
      workerLastHeartbeatTS.remove(workerAttemptId);
      context.getEventHandler()
          .handle(new WorkerAttemptDiagnosticsUpdateEvent(workerAttemptId, request.getMsg()));
      context.getEventHandler().handle(new WorkerAttemptEvent(WorkerAttemptEventType.ERROR, workerAttemptId));
      resBuilder.setCommand(WorkerCommandProto.W_SUCCESS);
    }

    return resBuilder.build();
  }

  public List<MatrixMeta> getMatrics() {
    return matrics;
  }

  /**
   * remove worker attempt from monitor set
   * @param workerAttemptId worker attempt id
   */
  public void unRegisterWorkerAttemptId(WorkerAttemptId workerAttemptId) {
    LOG.info(workerAttemptId + " is unregistered in monitor!");
    workerLastHeartbeatTS.remove(workerAttemptId);
  }

  /**
   * add worker attempt to monitor set
   * @param workerAttemptId worker attempt id
   */
  public void registerWorkerAttemptId(WorkerAttemptId workerAttemptId) {
    LOG.info(workerAttemptId + " is registered in monitor!");
    workerLastHeartbeatTS.put(workerAttemptId, System.currentTimeMillis());
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
    MatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();
    int size = needSaveMatrices.size();
    for(int i = 0; i < size; i++) {
      MatrixProto matrixMeta = matrixMetaManager.getMatrix(needSaveMatrices.get(i));
      if(matrixMeta == null) {
        throw new ServiceException("matrix " + needSaveMatrices.get(i) + " does not exist");
      }
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
    int exitStatus = request.getExitStatus();
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

    return ClientMasterServiceProtos.StopResponse.newBuilder().build();
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
      if(context.getMatrixMetaManager().isCreated(names.get(i))) {
        builder.addStatus(MatrixStatus.M_OK);
      } else {
        builder.addStatus(MatrixStatus.M_NOT_READY);
      }
    }

    return builder.build();
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
}
