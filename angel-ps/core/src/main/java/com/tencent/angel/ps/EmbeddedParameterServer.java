package com.tencent.angel.ps;

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.psagent.matrix.transport.LocalTransportClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.*;

public class EmbeddedParameterServer {
  private static final Log LOG = LogFactory.getLog(EmbeddedParameterServer.class);
  private ParameterServer psServer;
  private PSContext psContext;
  private LocalTransportClient localPSClient;
  private static volatile EmbeddedParameterServer instance;

  private final Map<Integer, List<PartitionKey>> taskIdToPartMap = new HashMap<>();
  private final Set<PartitionKey> assignedParts = new HashSet<>();

  public static EmbeddedParameterServer getInstance() {
    if (instance == null) {
      synchronized (EmbeddedParameterServer.class) {
        if (instance == null) {
          instance = new EmbeddedParameterServer();
        }
      }
    }
    return instance;
  }

  public synchronized ParameterServerId getPSId() {
    if (psServer == null) {
      throw new AngelException("PS server not init");
    }
    return psServer.getServerId();
  }

  public List<PartitionKey> assignPSParts(int matrixId, int taskId, int needNum) {
    synchronized (this) {
      List<PartitionKey> partKeys = taskIdToPartMap.get(taskId);
      if(partKeys == null) {
        partKeys = new ArrayList<>(needNum);
        // Assign
        MatrixMeta matrixMeta = psServer.getMatrixMetaManager()
            .getMatrixMeta(matrixId);
        if(matrixMeta == null) {
          throw new AngelException("Can not find matrix " + matrixId + " on PS");
        }
        PartitionKey[] parts = matrixMeta.getPartitionKeys();
        int assignCount = 0;
        for (PartitionKey part : parts) {
          if (!assignedParts.contains(part)) {
            partKeys.add(part);
            assignCount++;
            assignedParts.add(part);
            if (assignCount >= needNum) {
              break;
            }
          }
        }

        taskIdToPartMap.put(taskId, partKeys);
      }

      if(partKeys == null) {
        throw new AngelException("Can not find part for task " + taskId);
      }

      return partKeys;
    }
  }

  public void releasePSPart(int matrixId, int taskId, List<PartitionKey> parts) {
    synchronized (this) {
      taskIdToPartMap.remove(taskId);
      int partNum = parts.size();
      for (PartitionKey part : parts) {
        assignedParts.remove(part);
      }
    }
  }

  public void reset() {
    synchronized (this) {
      taskIdToPartMap.clear();
      assignedParts.clear();
    }
  }


  private int getAttemptIndexFromMaster(Configuration conf, int serverIndex,
                                        String appMasterHost, int appMasterPort) throws
      IOException, ServiceException {
    ParameterServerId psId = new ParameterServerId(serverIndex);
    TConnection conn = TConnectionManager.getConnection(conf);
    MasterProtocol masterProxy = conn.getMasterService(appMasterHost, appMasterPort);
    com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSGetAttemptIndexRequest.Builder regBuilder = com.tencent.angel.protobuf.generated.PSMasterServiceProtos
        .PSGetAttemptIndexRequest.newBuilder();
    regBuilder.setPsId(ProtobufUtil.convertToIdProto(psId));
    com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSGetAttemptIndexResponse res = masterProxy
        .psGetAttemptIndex(null, regBuilder.build());
    return res.getAttemptIndex();
  }

  private int assignPSIndex(Configuration conf) throws
      IOException, ServiceException {

    String appMasterHost = conf.get("angel.master.ip");
    int appMasterPort = Integer.parseInt(conf.get("angel.master.port"));

    TConnection conn = TConnectionManager.getConnection(conf);
    MasterProtocol masterProxy = conn.getMasterService(appMasterHost, appMasterPort);
    com.tencent.angel.protobuf.generated.PSMasterServiceProtos.AssignIndexRequest.Builder regBuilder = com.tencent.angel.protobuf.generated.PSMasterServiceProtos
        .AssignIndexRequest.newBuilder();
    com.tencent.angel.protobuf.generated.PSMasterServiceProtos.AssignIndexResponse res = masterProxy
        .assignIndex(null, regBuilder.build());
    return res.getIndex();
  }

  public synchronized boolean run(Map<String, String> angelMap) throws IOException,
      ServiceException {
    if (psServer == null) {
      ParameterServer ps = startPs(angelMap);
      psContext = ps.getContext();
      localPSClient = new LocalTransportClient(psContext);
      psServer = ps;
      return true;
    } else {
      LOG.info("PS " + psServer.getServerId().getIndex() + " already start");
      System.out.println("PS " + psServer.getServerId().getIndex() + " already start");
      return false;
    }
  }

  private ParameterServer startPs(Map<String, String> angelMap)
      throws IOException, ServiceException {
    LOG.info("Starting Parameter Server...");
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : angelMap.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    // Assign ps index from master, ps attempt always 0
    int index = assignPSIndex(conf);
    int attemptIndex = 0;

    conf.set("angel.ps.index", String.valueOf(index));

    LOG.info("Parameter Server ID is: " + index + ", attempt index is: " + attemptIndex);
    String appMasterHost = conf.get("angel.master.ip");
    int appMasterPort = Integer.parseInt(conf.get("angel.master.port"));
    ParameterServer ps =
        new ParameterServer(index, attemptIndex, appMasterHost, appMasterPort, conf);
    try {
      ps.initialize();
      ps.start();
    } catch (Throwable x) {
      LOG.fatal("Start PS failed ", x);
      ps.failed(x.getMessage());
    }
    LOG.info("Starting Parameter Server successfully.");
    System.out.println("Starting Parameter Server successfully.");
    return ps;
  }

  public synchronized ParameterServer getPsServer() {
    return psServer;
  }

  public synchronized PSContext getPsContext() {
    return psContext;
  }

  public synchronized LocalTransportClient getLocalPSClient() {
    return localPSClient;
  }

  public void stop() {
    synchronized (this) {
      LOG.info("now stop ps.");
      if (psServer != null) {
        psServer.stop(0);
        psServer = null;
        LOG.info("stop ps successfully.");
        System.out.println("stop ps successfully.");
      } else {
        LOG.info("PS  has already stopped.");
        System.out.println("PS  has already stopped.");
      }
    }
  }
}
