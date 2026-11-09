package com.tencent.angel.client.embedded;

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.AngelApplicationMaster;
import com.tencent.angel.master.yarn.util.AngelApps;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.StartRequest;
import java.io.IOException;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public class AngelEmbeddedClient extends AngelClient {

  private static final Log LOG = LogFactory.getLog(AngelEmbeddedClient.class);

  @Override
  public void addMatrix(MatrixContext mContext) throws AngelException {
    super.addMatrix(mContext);
  }

  /**
   * application id
   */
  private ApplicationId appId;

  /**
   * Create a new AngelStandaloneClient.
   *
   * @param conf application configuration
   */
  public AngelEmbeddedClient(Configuration conf) {
    super(conf);
  }

  @Override
  public void startPSServer() throws AngelException {
    try {
      setUser();
      //setLocalAddr();
      Path stagingDir = AngelApps.getStagingDir(conf, userName);

      // 2.get job id
      long clusterTimestamp = System.currentTimeMillis();
      int randomId = new Random().nextInt(99999);
      appId = ApplicationId.newInstance(clusterTimestamp, randomId);
      JobID jobId = TypeConverter.fromYarn(appId);

      Path submitJobDir = new Path(stagingDir, appId.toString());

      conf.set(AngelConf.ANGEL_JOB_DIR, submitJobDir.toString());
      conf.set(AngelConf.ANGEL_JOB_ID, jobId.toString());
      conf.set(AngelConf.ANGEL_EMBEDDED_APP_ID, appId.toString());

      setInputDirectory();
      setOutputDirectory();

      checkParameters(conf);
      handleDeprecatedParameters(conf);

      this.appId = ApplicationId.newInstance(System.currentTimeMillis(), new Random().nextInt());
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 0);

      LocalClusterContext clusterContext = LocalClusterContext.get();
      clusterContext.setConf(conf);
      clusterContext.setLocalHost("localhost");
      clusterContext.setPort(9999);
      clusterContext.setHttpPort(8888);
      clusterContext.setAppId(appId);

      conf.setBoolean("fs.automatic.close", false);
      String appName = conf.get(AngelConf.ANGEL_JOB_NAME, "mpi");

      AngelApplicationMaster appMaster =
          new AngelApplicationMaster(conf, appName, appAttemptId, clusterContext.getContainerId(),
              clusterContext.getLocalHost(), clusterContext.getPort(), clusterContext.getHttpPort(),
              System.currentTimeMillis(), new Credentials());

      appMaster.initAndStart();
      masterLocation = appMaster.getMasterLocation();

      updateMaster(10);
    } catch (Exception x) {
      LOG.error("start angel master in standalone mode failed.", x);
      throw new AngelException(x);
    }
  }

  @Override
  public void run() throws AngelException {
    if (master == null) {
      throw new AngelException(
          "parameter servers are not started, you must execute startPSServer first!!");
    }

    try {
      master.start(null, StartRequest.newBuilder().build());
    } catch (ServiceException e) {
      LOG.error("start application failed.", e);
      throw new AngelException(e);
    }
  }

  private static void validateInputParam(String value, String param) throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  @Override
  public void kill() {
    System.exit(-1);
  }

  @Override
  public void close() {

  }


  @Override
  protected void updateMaster(int maxWaitSeconds) throws Exception {
    int tryTime = 0;
    TConnection connection = TConnectionManager.getConnection(conf);
    while (tryTime < maxWaitSeconds) {
      try {
        LOG.info("master host=" + masterLocation.getIp() + ", port=" + masterLocation.getPort());
        LOG.info("start to create rpc client to am");
        Thread.sleep(5000);
        master = connection.getMasterService(masterLocation.getIp(), masterLocation.getPort());
        startHeartbeat();
      } catch (Exception e) {
        LOG.error("Register to Master failed, ", e);
        Thread.sleep(1000);
        tryTime++;
        continue;
      }
      break;
    }

    if (tryTime >= maxWaitSeconds && masterLocation == null) {
      throw new IOException("wait for master location timeout");
    }
  }

  @Override
  protected String getAppId() {
    return appId.toString();
  }
}
