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


package com.tencent.angel.client.kubernetes;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.kubernetesmanager.deploy.submit.KubernetesClientApplication;
import com.tencent.angel.master.yarn.util.AngelApps;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.worker.WorkerId;
import java.io.IOException;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Angel client used on KUBERNETES deploy mode.
 */
public class AngelKubernetesClient extends AngelClient {

  private static final Log LOG = LogFactory.getLog(AngelKubernetesClient.class);

  /**
   * used for upload application resource files
   */
  private FileSystem jtFs;

  /**
   * Kubernetes client app for start angel master
   */
  private KubernetesClientApplication k8sClientApp;

  @Override
  public void addMatrix(MatrixContext mContext) throws AngelException {
    super.addMatrix(mContext);
  }

  /**
   * application id
   */
  private ApplicationId appId;

  final public static FsPermission JOB_DIR_PERMISSION = FsPermission.createImmutable((short) 0777);

  /**
   * Create a new AngelYarnClient.
   *
   * @param conf application configuration
   */
  public AngelKubernetesClient(Configuration conf) {
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
      jtFs = submitJobDir.getFileSystem(conf);

      conf.set(AngelConf.ANGEL_JOB_DIR, submitJobDir.toString());
      conf.set(AngelConf.ANGEL_JOB_ID, jobId.toString());
      conf.set(AngelConf.ANGEL_KUBERNETES_APP_ID, appId.toString());
      conf.setLong(AngelConf.ANGEL_KUBERNETES_APP_CLUSTERTIMESTAMP, clusterTimestamp);
      conf.setInt(AngelConf.ANGEL_KUBERNETES_APP_RANDOMID, randomId);

      setInputDirectory();
      setOutputDirectory();

      checkParameters(conf);
      handleDeprecatedParameters(conf);

      // 5.write configuration to a xml file
      Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
      writeConf(conf, submitJobFile);

      k8sClientApp = new KubernetesClientApplication();
      /**
       * Launch master and executor thread
       */
      Thread launchMasterPodThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            k8sClientApp.run(conf);
          } catch (Exception e) {
            LOG.error("launch master pod exception");
            e.printStackTrace();
            throw e;
          }
        }
      });
      launchMasterPodThread.start();

      // 8.get app master client
      updateMaster(10 * 60);

      waitForAllPS(conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER));
      LOG.info("start pss success");
    } catch (Exception x) {
      LOG.error("submit application to kubernetes cluster failed.", x);
      throw new AngelException(x);
    }
  }

  @Override
  public void kill() {
    System.exit(-1);
  }

  private void writeConf(Configuration conf, Path jobFile) throws IOException {
    // Write job file to JobTracker's fs
    FSDataOutputStream out =
        FileSystem.create(jtFs, jobFile, new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }

  @Override
  protected void updateMaster(int maxWaitSeconds) throws Exception {
    int port;
    int tryTime = 0;
    TConnection connection = TConnectionManager.getConnection(conf);
    while (tryTime < maxWaitSeconds) {
      String masterPodIp = k8sClientApp.getAngelMasterPodIp();
      port = conf.getInt(AngelConf.ANGEL_KUBERNETES_MASTER_PORT,
          AngelConf.DEFAULT_ANGEL_KUBERNETES_MASTER_PORT);
      if (masterPodIp == null || "".equals(masterPodIp)) {
        LOG.info("AM not assigned to Job. Waiting to get the AM ...");
        Thread.sleep(1000);
        tryTime++;
      } else {
        try {
          masterLocation = new Location(masterPodIp, port);
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
    }

    if (tryTime >= maxWaitSeconds && masterLocation == null) {
      throw new IOException("wait for master location timeout");
    }
  }

  @Override
  protected String getAppId() {
    return appId.toString();
  }

  @Override
  protected void printWorkerLogUrl(WorkerId workerId) {
  }
}

