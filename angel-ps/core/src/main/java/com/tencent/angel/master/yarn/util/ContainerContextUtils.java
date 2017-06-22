/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Container environment variables are modified to satisfy Angel worker/ps.
 */

package com.tencent.angel.master.yarn.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.master.MasterService;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.utils.NetUtils;
import com.tencent.angel.worker.WorkerAttemptId;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * Yarn container context utils
 */
public class ContainerContextUtils {
  private static final Log LOG = LogFactory.getLog(ContainerContextUtils.class);
  
  private static String initialClasspath = null;
  private static String initialAppClasspath = null;
  private static final Object classpathLock = new Object();
  private static final AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  
  private static final Object commonContainerSpecLock = new Object();
  private static ContainerLaunchContext commonContainerSpec = null;
  
  public
  static ContainerLaunchContext createContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs, Configuration conf, WorkerAttemptId workerAttemptId,
      int initMinClock, final ApplicationId appid,
      MasterService masterService, Credentials credentials) {

    synchronized (commonContainerSpecLock) {
      if (commonContainerSpec == null) {
        commonContainerSpec =
            createCommonContainerLaunchContext(masterService, applicationACLs, conf,
                appid, credentials);
      }
    }

    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);
    Apps.addToEnvironment(myEnv, AngelEnvironment.WORKER_ID.name(),
        Integer.toString(workerAttemptId.getWorkerId().getIndex()));
    Apps.addToEnvironment(myEnv, AngelEnvironment.WORKER_ATTEMPT_ID.name(),
        Integer.toString(workerAttemptId.getIndex()));
    Apps.addToEnvironment(myEnv, AngelEnvironment.WORKER_GROUP_ID.name(),
        Integer.toString(workerAttemptId.getWorkerId().getWorkerGroupId().getIndex()));
    Apps.addToEnvironment(myEnv, AngelEnvironment.INIT_MIN_CLOCK.name(),
        Integer.toString(initMinClock));
    Apps.addToEnvironment(myEnv, AngelEnvironment.ANGEL_USER_TASK.name(), conf.get(
        AngelConfiguration.ANGEL_TASK_USER_TASKCLASS,
        AngelConfiguration.DEFAULT_ANGEL_TASK_USER_TASKCLASS));

    WorkerJVM.setVMEnv(myEnv, conf);

    // Set up the launch command
    List<String> commands = WorkerJVM.getVMCommand(conf, appid, workerAttemptId);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec.getServiceData().entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container
    ContainerLaunchContext container =
        ContainerLaunchContext.newInstance(commonContainerSpec.getLocalResources(), myEnv,
            commands, myServiceData, commonContainerSpec.getTokens().duplicate(), applicationACLs);

    return container;
  }

  private static ContainerLaunchContext createCommonContainerLaunchContext(
      MasterService masterService, Map<ApplicationAccessType, String> applicationACLs,
      Configuration conf, final ApplicationId appid, Credentials credentials) {

    // Application resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    // Application environment
    Map<String, String> environment = new HashMap<String, String>();

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();

    // Tokens
    ByteBuffer taskCredentialsBuffer = ByteBuffer.wrap(new byte[] {});
    try {
      FileSystem remoteFS = FileSystem.get(conf);

      // Set up JobConf to be localized properly on the remote NM.
      Path remoteJobSubmitDir = new Path(conf.get(AngelConfiguration.ANGEL_JOB_DIR));
      Path remoteJobConfPath = new Path(remoteJobSubmitDir, AngelConfiguration.ANGEL_JOB_CONF_FILE);
      localResources.put(
          AngelConfiguration.ANGEL_JOB_CONF_FILE,
          createLocalResource(remoteFS, remoteJobConfPath, LocalResourceType.FILE,
              LocalResourceVisibility.APPLICATION));
      LOG.info("The job-conf file on the remote FS is " + remoteJobConfPath.toUri().toASCIIString());

      LOG.info("actual workergroup number:"
          + conf.get(AngelConfiguration.ANGEL_WORKERGROUP_ACTUAL_NUM));
      LOG.info("actual task number:" + conf.get(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM));

      // Setup DistributedCache
      AngelApps.setupDistributedCache(conf, localResources);

      // Setup up task credentials buffer
      LOG.info("Adding #" + credentials.numberOfTokens() + " tokens and #"
          + credentials.numberOfSecretKeys() + " secret keys for NM use for launching container");

      Credentials taskCredentials = new Credentials(credentials);

      // LocalStorageToken is needed irrespective of whether security is enabled
      // or not.
      // TokenCache.setJobToken(jobToken, taskCredentials);

      DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
      LOG.info("Size of containertokens_dob is " + taskCredentials.numberOfTokens());
      taskCredentials.writeTokenStorageToStream(containerTokens_dob);
      taskCredentialsBuffer =
          ByteBuffer.wrap(containerTokens_dob.getData(), 0, containerTokens_dob.getLength());

      InetSocketAddress listenAddr = NetUtils.getRealLocalAddr(masterService.getRPCListenAddr());
      Apps.addToEnvironment(environment, AngelEnvironment.LISTEN_ADDR.name(), listenAddr
          .getAddress().getHostAddress());

      Apps.addToEnvironment(environment, AngelEnvironment.LISTEN_PORT.name(),
          String.valueOf(listenAddr.getPort()));

      String workerGroupNumStr = conf.get(AngelConfiguration.ANGEL_WORKERGROUP_ACTUAL_NUM);
      if(workerGroupNumStr != null) {
        Apps.addToEnvironment(environment, AngelEnvironment.WORKERGROUP_NUMBER.name(),
            conf.get(AngelConfiguration.ANGEL_WORKERGROUP_ACTUAL_NUM));
      }

      String taskNumStr = conf.get(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM);
      if(taskNumStr != null){
        Apps.addToEnvironment(environment, AngelEnvironment.TASK_NUMBER.name(),
            conf.get(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM));
      }

      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), getInitialClasspath(conf));

      if (initialAppClasspath != null) {
        Apps.addToEnvironment(environment, Environment.APP_CLASSPATH.name(), initialAppClasspath);
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    Apps.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(), Environment.PWD.$());

    // Construct the actual Container
    // The null fields are per-container and will be constructed for each
    // container separately.
    ContainerLaunchContext container =
        ContainerLaunchContext.newInstance(localResources, environment, null, serviceData,
            taskCredentialsBuffer, applicationACLs);

    return container;
  }

  @SuppressWarnings("deprecation")
  public static ContainerLaunchContext createContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs, Configuration conf,
      PSAttemptId psAttemptId, final ApplicationId appid, MasterService masterService,
      Credentials credentials) {

    synchronized (commonContainerSpecLock) {
      if (commonContainerSpec == null) {
        commonContainerSpec =
            createCommonContainerLaunchContext(masterService, applicationACLs, conf,
                appid, credentials);
      }
    }

    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);

    Apps.addToEnvironment(myEnv, AngelEnvironment.PARAMETERSERVER_ID.name(),
        Integer.toString(psAttemptId.getParameterServerId().getIndex()));
    Apps.addToEnvironment(myEnv, AngelEnvironment.PS_ATTEMPT_ID.name(),
        Integer.toString(psAttemptId.getIndex()));

    ParameterServerJVM.setVMEnv(myEnv, conf);

    // Set up the launch command
    List<String> commands = ParameterServerJVM.getVMCommand(conf, appid, psAttemptId);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec.getServiceData().entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container
    ContainerLaunchContext container =
        ContainerLaunchContext.newInstance(commonContainerSpec.getLocalResources(), myEnv,
            commands, myServiceData, commonContainerSpec.getTokens().duplicate(), applicationACLs);

    return container;
  }
 
  private static LocalResource createLocalResource(FileSystem fc, Path file,
      LocalResourceType type, LocalResourceVisibility visibility) throws IOException {
    FileStatus fstat = fc.getFileStatus(file);
    URL resourceURL = ConverterUtils.getYarnUrlFromPath(fc.resolvePath(fstat.getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return LocalResource.newInstance(resourceURL, type, visibility, resourceSize,
        resourceModificationTime);
  }

  private static String getInitialClasspath(Configuration conf) throws IOException {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<String, String>();
      AngelApps.setClasspath(env, conf);
      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialAppClasspath = env.get(Environment.APP_CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }
}
