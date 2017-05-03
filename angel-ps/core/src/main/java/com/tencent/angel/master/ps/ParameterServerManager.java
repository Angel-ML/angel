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

package com.tencent.angel.master.ps;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.AppEvent;
import com.tencent.angel.master.app.AppEventType;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.matrix.committer.AMMatrixCommitter;
import com.tencent.angel.master.ps.ps.AMParameterServer;
import com.tencent.angel.master.ps.ps.AMParameterServerEvent;
import com.tencent.angel.master.ps.ps.AMParameterServerEventType;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.utils.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

/**
 * Parameter server manager, it managers a group of
 * {@link com.tencent.angel.master.ps.ps.AMParameterServer}. It is responsible for starting all the
 * parameter servers. If all the workers have completed the model training, it will inform the
 * parameter servers to write models to the corresponding file, and finally merge them use
 * {@link com.tencent.angel.master.matrix.committer.AMMatrixCommitter}
 */
public class ParameterServerManager extends AbstractService implements
    EventHandler<ParameterServerManagerEvent> {

  private static final Log LOG = LogFactory.getLog(ParameterServerManager.class);
  private final AMContext context;
  
  /**parameter server number*/
  private final int psNumber;
  
  /**
   * If we need to suggest physical machines which the parameter servers will running on, we can
   * specify the physical machine ip list
   */
  private final String[] ips;
  
  /**the amount of resources requested for each parameter server*/
  private final Resource psResource;
  
  /**the resource priority for parameter servers*/
  private final Priority priority;
  
  /**parameter server id to parameter server management unit map*/
  private final Map<ParameterServerId, AMParameterServer> psMap;
  
  /**The parameter server collection that has completed the commit operation*/
  private final Set<ParameterServerId> committedPs;
  
  /**model output path*/
  private final Path outputPath;
  
  /**temporary model output path*/
  private final Path tmpOutputPath;
  
  /**model committer, it move model files to output path*/
  private AMMatrixCommitter committer;
  
  /**whether you can start commit operation*/
  private final AtomicBoolean canCommit;
  
  /**need commit matrices*/
  private volatile List<Integer> needCommitMatrixIds;
  
  /**parameter server id to attempt index map, it use to master recover*/
  private final Map<ParameterServerId, Integer> psIdToAttemptIndexMap;

  public ParameterServerManager(AMContext context, Map<ParameterServerId, Integer> psIdToAttemptIndexMap) {
    super(ParameterServerManager.class.getName());
    this.context = context;
    this.psIdToAttemptIndexMap = psIdToAttemptIndexMap;
    Configuration conf = context.getConf();
    String ipListStr = conf.get(AngelConfiguration.ANGEL_PS_IP_LIST);
    if (ipListStr != null) {
      ips = ipListStr.split(",");
      psNumber = ips.length;
    } else {
      ips = null;
      psNumber =
          conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER,
              AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);
    }

    int psServerMemory =
        conf.getInt(AngelConfiguration.ANGEL_PS_MERMORY_MB,
            AngelConfiguration.DEFAULT_ANGEL_PS_MERMORY_MB);

    int psServerVcores =
        conf.getInt(AngelConfiguration.ANGEL_PS_CPU_VCORES,
            AngelConfiguration.DEFAULT_ANGEL_PS_CPU_VCORES);

    int psPriority =
        conf.getInt(AngelConfiguration.ANGEL_PS_PRIORITY,
            AngelConfiguration.DEFAULT_ANGEL_PS_PRIORITY);

    psResource = Resource.newInstance(psServerMemory, psServerVcores);
    priority = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    priority.setPriority(psPriority);

    psMap = new HashMap<ParameterServerId, AMParameterServer>();
    committedPs = new HashSet<ParameterServerId>();

    String outputPathStr = conf.get(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH);
    String tmpOutputPathStr = conf.get(AngelConfiguration.ANGEL_JOB_TMP_OUTPUT_DIRECTORY);
    outputPath = new Path(outputPathStr);
    tmpOutputPath = new Path(tmpOutputPathStr);

    canCommit = new AtomicBoolean(false);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (committer != null) {
      committer.stop();
    }
    super.serviceStop();
  }

  /**
   * init and start parameter servers
   */
  public void startAllPS() {
    for (int i = 0; i < psNumber; i++) {
      ParameterServerId id = new ParameterServerId(i);
      AMParameterServer server = null;
      if (ips != null) {
        server = new AMParameterServer(ips[i], id, context);
      } else {
        server = new AMParameterServer(id, context);
      }

      if(psIdToAttemptIndexMap != null && psIdToAttemptIndexMap.containsKey(id)) {
        server.setNextAttemptNumber(psIdToAttemptIndexMap.get(id));
      }
      psMap.put(id, server);
      server.handle(new AMParameterServerEvent(AMParameterServerEventType.PS_SCHEDULE, id));
    }
  }

  /**
   * get parameter servers map
   * @return Map<ParameterServerId, AMParameterServer> parameter servers map
   */
  public Map<ParameterServerId, AMParameterServer> getParameterServerMap() {
    return psMap;
  }

  @Override
  public void handle(ParameterServerManagerEvent event) {
    LOG.debug("Processing event type " + event.getType());
    switch (event.getType()) {
      case COMMIT: {
        LOG.info("set canCommit to true.");
        canCommit.set(true);
        needCommitMatrixIds = ((CommitEvent) event).getNeedCommitMatrixIds();
        break;
      }

      case PARAMETERSERVER_DONE: {
        commitSuccess(event);
        break;
      }

      case PARAMETERSERVER_FAILED: {
        psFailed(event);
        break;
      }

      case PARAMETERSERVER_KILLED: {
        psKilled(event);
        break;
      }

      default:
        break;
    }
  }

  /**
   * Check whether we can start the commit operation
   * @return boolean true indicates that a commit operation can be performed
   */
  public boolean psCanCommit() {
    return canCommit.get();
  }

  /**
   * get parameter server manager unit use id
   * @param id parameter server id
   * @return AMParameterServer parameter server manager unit
   */
  public AMParameterServer getParameterServer(ParameterServerId id) {
    return psMap.get(id);
  }

  /**
   * get parameter server number
   * @return int parameter server number
   */
  public int getPsNumber() {
    return psNumber;
  }

  /**
   * get parameter server resource allocation
   * @return Resource parameter server resource allocation
   */
  public Resource getPsResource() {
    return psResource;
  }

  /**
   * get parameter server resource priority
   * @return Priority parameter server resource priority
   */
  public Priority getPriority() {
    return priority;
  }
  
  @SuppressWarnings("unchecked")
  private void psKilled(ParameterServerManagerEvent event) {
    context.getEventHandler().handle(new AppEvent(context.getApplicationId(), AppEventType.KILL));
  }

  @SuppressWarnings("unchecked")
  private void psFailed(ParameterServerManagerEvent event) {
    List<String> diagnostics =
        context.getParameterServerManager().getParameterServer(event.getPsId()).getDiagnostics();
    StringBuffer sb = new StringBuffer();
    sb.append(StringUtils.join("\n", diagnostics));
    context.getEventHandler().handle(
        new InternalErrorEvent(context.getApplicationId(), sb.toString()));
  }

  private void commitSuccess(ParameterServerManagerEvent event) {
    committedPs.add(event.getPsId());
    //if all parameter server complete commit, master can commit now
    if (committedPs.size() == psNumber) {
      commit();
    }
  }

  private void commit() {
    //init and start master committer
    committer = new AMMatrixCommitter(context, outputPath, tmpOutputPath);
    committer.init(context.getConf());
    committer.start();
  }


  /**
   * Get the matrices that need commit.
   * 
   * @return List<Integer> matrices that need commit.
   */
  public List<Integer> getNeedCommitMatrixIds() {
    return needCommitMatrixIds;
  }
}
