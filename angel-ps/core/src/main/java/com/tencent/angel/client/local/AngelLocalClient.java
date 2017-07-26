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

package com.tencent.angel.client.local;

import java.util.Random;

import com.tencent.angel.conf.AngelConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalCluster;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.localcluster.LocalMaster;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.PingRequest;

/**
 * Angel client used on LOCAL deploy mode.
 */
public class AngelLocalClient extends AngelClient {
  private static final Log LOG = LogFactory.getLog(AngelLocalClient.class);
  /**application id*/
  private final ApplicationId appId;
  
  /**local cluster*/
  private LocalCluster cluster;

  /**
   * 
   * Create a new AngelLocalClient.
   *
   * @param conf application configuration
   */
  public AngelLocalClient(Configuration conf) {
    super(conf);
    this.appId = ApplicationId.newInstance(System.currentTimeMillis(), new Random().nextInt());
  }
  
  private void initLocalClusterContext() {  
    LocalClusterContext localClusterContext = LocalClusterContext.get();
    localClusterContext.setConf(conf);
    localClusterContext.setLocalHost("127.0.0.1");
    localClusterContext.setPort(9999);
    localClusterContext.setHttpPort(8888);
    localClusterContext.setAppId(appId);  
  }
  
  @Override
  protected void updateMaster(int maxWaitSeconds) throws Exception {
    int tryTime = 0;
    TConnection connection = TConnectionManager.getConnection(conf);
    while (tryTime < maxWaitSeconds) {
      LocalMaster localMaster = LocalClusterContext.get().getMaster();
      if(localMaster == null || localMaster.getAppMaster().getAppContext().getMasterService() == null) {
        Thread.sleep(1000);
        tryTime++;
        continue;
      }
      
      masterLocation = localMaster.getAppMaster().getAppContext().getMasterService().getLocation();
      if(masterLocation == null) {
        Thread.sleep(1000);
        tryTime++;
        continue;
      }
      
      try {
        LOG.info("start to create rpc client to am");
        master = connection.getMasterService(masterLocation.getIp(), masterLocation.getPort());
        master.ping(null, PingRequest.newBuilder().build());
        break;
      } catch (ServiceException e) {
        Thread.sleep(1000);
        tryTime++;
      }
    }
  }

  @Override
  public void stop() throws AngelException{
    if(cluster != null) {
      cluster.stop();
    }
    close();
    cleanLocalClusterContext();
  }
  
  private void cleanLocalClusterContext() {
    LocalClusterContext.get().clear();
  }

  @Override
  protected String getAppId() {
    return appId.toString();
  }
  
  @Override
  public void startPSServer() throws AngelException {
    try{
      setUser();
      setLocalAddr();
      
      conf.set("hadoop.http.filter.initializers",
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");

      setOutputDirectory();
      initLocalClusterContext();
      
      cluster = new LocalCluster(conf, appId);
      cluster.start();
      updateMaster(Integer.MAX_VALUE);
      waitForAllPS(conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER));
      LOG.info("start ps success");
    } catch (Exception x) {
      LOG.error("start application failed.", x);
      throw new AngelException(x);
    }
  }
}
