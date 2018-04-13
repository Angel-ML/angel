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
 * The job submission process and related configuration have been modified according to the specific
 * circumstances of Angel.
 */

package com.tencent.angel.client.yarn;

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.yarn.util.AngelApps;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.PingRequest;

import com.tencent.angel.worker.WorkerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Angel client used on YARN deploy mode.
 */
public class AngelYarnClient extends AngelClient {
  private static final Log LOG = LogFactory.getLog(AngelYarnClient.class);
  
  /**used for upload application resource files*/
  private FileSystem jtFs;
  
  /**the tokens to access YARN resourcemanager*/
  private final Credentials credentials;
  
  /**rpc to YARN record factory*/
  private RecordFactory recordFactory;

  @Override
  public void addMatrix(MatrixContext mContext) throws AngelException {
    super.addMatrix(mContext);
  }

  /**rpc client to YARN resourcemanager*/
  private YarnClient yarnClient;
  
  /**YARN application id*/
  private ApplicationId appId;
  
  private static final String UNAVAILABLE = "N/A";
  final public static FsPermission JOB_DIR_PERMISSION = FsPermission.createImmutable((short) 0777);

  /**
   * 
   * Create a new AngelYarnClient.
   *
   * @param conf application configuration
   */
  public AngelYarnClient(Configuration conf) {
    super(conf);
    credentials = new Credentials();
  }
  
  @Override
  public void startPSServer() throws AngelException {
    try {
      setUser();
      setLocalAddr();
      Path stagingDir = AngelApps.getStagingDir(conf, userName);

      // 2.get job id
      yarnClient = YarnClient.createYarnClient();
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      yarnClient.init(yarnConf);
      yarnClient.start();
      YarnClientApplication newApp;

      newApp = yarnClient.createApplication();
      GetNewApplicationResponse newAppResponse = newApp.getNewApplicationResponse();
      appId = newAppResponse.getApplicationId();
      JobID jobId = TypeConverter.fromYarn(appId);

      Path submitJobDir = new Path(stagingDir, appId.toString());
      jtFs = submitJobDir.getFileSystem(conf);

      conf.set("hadoop.http.filter.initializers",
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      conf.set(AngelConf.ANGEL_JOB_DIR, submitJobDir.toString());
      conf.set(AngelConf.ANGEL_JOB_ID, jobId.toString());

      setInputDirectory();
      setOutputDirectory();

      // Credentials credentials = new Credentials();
      credentials.addAll(UserGroupInformation.getCurrentUser().getCredentials());
      TokenCache.obtainTokensForNamenodes(credentials, new Path[] {submitJobDir}, conf);
      checkParameters(conf);
      handleDeprecatedParameters(conf);

      // 4.copy resource files to hdfs
      copyAndConfigureFiles(conf, submitJobDir, (short) 10);

      // 5.write configuration to a xml file
      Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
      TokenCache.cleanUpTokenReferral(conf);
      writeConf(conf, submitJobFile);

      // 6.create am container context
      ApplicationSubmissionContext appContext =
          createApplicationSubmissionContext(conf, submitJobDir, credentials, appId);

      conf.set(AngelConf.ANGEL_JOB_LIBJARS, "");

      // 7.Submit to ResourceManager
      appId = yarnClient.submitApplication(appContext);

      // 8.get app master client
      updateMaster(10 * 60);
      
      waitForAllPS(conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER));
      LOG.info("start pss success");
    } catch (Exception x) {
      LOG.error("submit application to yarn failed.", x);
      throw new AngelException(x);
    }
  }

  @Override
  public void stop() throws AngelException{
    super.stop();
    if (yarnClient != null) {
      try {
        yarnClient.killApplication(appId);
      } catch (YarnException | IOException e) {
        throw new AngelException(e);
      }
      yarnClient.stop();
    }
    close();
  }

  @Override
  public void stop(int stateCode) throws AngelException{
    LOG.info("stop the application");
    super.stop();
    if(master != null) {
      try {
        LOG.info("master is not null, send stop command to Master, stateCode=" + stateCode);
        master.stop(null, ClientMasterServiceProtos.StopRequest.newBuilder().setExitStatus(stateCode).build());
      } catch (ServiceException e) {
        LOG.error("send stop command to Master failed ", e);
        stop();
        throw new AngelException(e);
      }
      close();
    } else {
      LOG.info("master is null, just kill the application");
      stop();
    }
  }
  private void copyAndConfigureFiles(Configuration conf, Path submitJobDir, short i)
      throws IOException {

    String files = conf.get(AngelConf.ANGEL_JOB_CACHE_FILES);
    String libjars = conf.get(AngelConf.ANGEL_JOB_LIBJARS);
    String archives = conf.get(AngelConf.ANGEL_JOB_CACHE_ARCHIVES);

    // Create a number of filenames in the JobTracker's fs namespace
    LOG.info("default FileSystem: " + jtFs.getUri());
    if (jtFs.exists(submitJobDir)) {
      throw new IOException("Not submitting job. Job directory " + submitJobDir
          + " already exists!! This is unexpected.Please check what's there in" + " that directory");
    }
    submitJobDir = jtFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission angelSysPerms = new FsPermission(JOB_DIR_PERMISSION);
    FileSystem.mkdirs(jtFs, submitJobDir, angelSysPerms);
    Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
    Path archivesDir = JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
    Path libjarsDir = JobSubmissionFiles.getJobDistCacheLibjars(submitJobDir);

    LOG.info("libjarsDir=" + libjarsDir);
    LOG.info("libjars=" + libjars);
    // add all the command line files/ jars and archive
    // first copy them to jobtrackers filesystem

    if (files != null) {
      FileSystem.mkdirs(jtFs, filesDir, angelSysPerms);
      String[] fileArr = files.split(",");
      for (String tmpFile : fileArr) {
        URI tmpURI = null;
        try {
          tmpURI = new URI(tmpFile);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = copyRemoteFiles(filesDir, tmp, conf, i);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          DistributedCache.addCacheFile(pathURI, conf);
        } catch (URISyntaxException ue) {
          // should not throw a uri exception
          throw new IOException("Failed to create uri for " + tmpFile, ue);
        }
      }
    }

    if (libjars != null) {
      FileSystem.mkdirs(jtFs, libjarsDir, angelSysPerms);
      String[] libjarsArr = libjars.split(",");
      for (String tmpjars : libjarsArr) {
        Path tmp = new Path(tmpjars);
        Path newPath = copyRemoteFiles(libjarsDir, tmp, conf, i);
        DistributedCache.addFileToClassPath(new Path(newPath.toUri().getPath()), conf);
      }
    }

    if (archives != null) {
      FileSystem.mkdirs(jtFs, archivesDir, angelSysPerms);
      String[] archivesArr = archives.split(",");
      for (String tmpArchives : archivesArr) {
        URI tmpURI;
        try {
          tmpURI = new URI(tmpArchives);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = copyRemoteFiles(archivesDir, tmp, conf, i);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          DistributedCache.addCacheArchive(pathURI, conf);
        } catch (URISyntaxException ue) {
          // should not throw an uri excpetion
          throw new IOException("Failed to create uri for " + tmpArchives, ue);
        }
      }
    }

    // set the timestamps of the archives and files
    // set the public/private visibility of the archives and files
    ClientDistributedCacheManager.determineTimestampsAndCacheVisibilities(conf);
    // get DelegationToken for each cached file
    ClientDistributedCacheManager.getDelegationTokens(conf, credentials);
  }

  private Path copyRemoteFiles(Path parentDir, Path originalPath, Configuration conf,
      short replication) throws IOException {
    // check if we do not need to copy the files
    // is jt using the same file system.
    // just checking for uri strings... doing no dns lookups
    // to see if the filesystems are the same. This is not optimal.
    // but avoids name resolution.

    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(conf);
    if (compareFs(remoteFs, jtFs)) {
      return originalPath;
    }
    // this might have name collisions. copy will throw an exception
    // parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, conf);
    jtFs.setReplication(newPath, replication);
    return newPath;
  }

  private boolean compareFs(FileSystem srcFs, FileSystem destFs) {
    URI srcUri = srcFs.getUri();
    URI dstUri = destFs.getUri();
    if (srcUri.getScheme() == null) {
      return false;
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false;
    }
    String srcHost = srcUri.getHost();
    String dstHost = dstUri.getHost();
    if ((srcHost != null) && (dstHost != null)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch (UnknownHostException ue) {
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    } else if (srcHost == null && dstHost != null) {
      return false;
    } else if (srcHost != null && dstHost == null) {
      return false;
    }
    // check for ports
    return srcUri.getPort() == dstUri.getPort();
  }

  private URI getPathURI(Path destPath, String fragment) throws URISyntaxException {
    URI pathURI = destPath.toUri();
    if (pathURI.getFragment() == null) {
      if (fragment == null) {
        pathURI = new URI(pathURI.toString() + "#" + destPath.getName());
      } else {
        pathURI = new URI(pathURI.toString() + "#" + fragment);
      }
    }
    return pathURI;
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

  private ApplicationSubmissionContext createApplicationSubmissionContext(Configuration conf,
      Path jobSubmitPath, Credentials ts, ApplicationId appId) throws IOException {
    ApplicationId applicationId = appId;

    // Setup resource requirements
    recordFactory = RecordFactoryProvider.getRecordFactory(null);
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(conf.getInt(AngelConf.ANGEL_AM_MEMORY_GB,
        AngelConf.DEFAULT_ANGEL_AM_MEMORY_GB) * 1024);
    capability.setVirtualCores(conf.getInt(AngelConf.ANGEL_AM_CPU_VCORES,
        AngelConf.DEFAULT_ANGEL_AM_CPU_VCORES));
    System.out.println("AppMaster capability = " + capability);

    // Setup LocalResources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    Path jobConfPath = new Path(jobSubmitPath, AngelConf.ANGEL_JOB_CONF_FILE);

    FileContext defaultFileContext = FileContext.getFileContext(this.conf);

    localResources.put(AngelConf.ANGEL_JOB_CONF_FILE,
        createApplicationResource(defaultFileContext, jobConfPath, LocalResourceType.FILE));

    // Setup security tokens
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    dob.close();

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<String>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    long logSize = 0;
    String logLevel =
      conf.get(AngelConf.ANGEL_AM_LOG_LEVEL,
            AngelConf.DEFAULT_ANGEL_AM_LOG_LEVEL);
    AngelApps.addLog4jSystemProperties(logLevel, logSize, vargs);

    // Add AM user command opts
    String angelAppMasterUserOptions = conf.get(AngelConf.ANGEL_AM_JAVA_OPTS);
    if(angelAppMasterUserOptions == null) {
      angelAppMasterUserOptions = masterJVMOptions(conf);
    }

    vargs.add(angelAppMasterUserOptions);
    vargs.add(conf.get(AngelConf.ANGEL_AM_CLASS, AngelConf.DEFAULT_ANGEL_AM_CLASS));
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR
        + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR
        + ApplicationConstants.STDERR);

    Vector<String> vargsFinal = new Vector<String>(8);
    // Final command
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add(mergedCommand.toString());

    LOG.info("Command to launch container for ApplicationMaster is : " + mergedCommand);

    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    Map<String, String> environment = new HashMap<String, String>();
    AngelApps.setClasspath(environment, conf);

    // Setup the environment variables for Admin first
    // Setup the environment variables (LD_LIBRARY_PATH, etc)
    AngelApps.setEnvFromInputString(environment, conf.get(
        AngelConf.ANGEL_AM_ADMIN_USER_ENV,
        AngelConf.DEFAULT_ANGEL_AM_ADMIN_USER_ENV));

    AngelApps.setEnvFromInputString(environment,
        conf.get(AngelConf.ANGEL_AM_ENV, AngelConf.DEFAULT_ANGEL_AM_ENV));

    // Parse distributed cache
    AngelApps.setupDistributedCache(conf, localResources);

    Map<ApplicationAccessType, String> acls = new HashMap<ApplicationAccessType, String>(2);
    acls.put(ApplicationAccessType.VIEW_APP, conf.get(AngelConf.JOB_ACL_VIEW_JOB,
        AngelConf.DEFAULT_JOB_ACL_VIEW_JOB));
    acls.put(ApplicationAccessType.MODIFY_APP, conf.get(AngelConf.JOB_ACL_MODIFY_JOB,
        AngelConf.DEFAULT_JOB_ACL_MODIFY_JOB));

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(localResources, environment, vargsFinal, null,
            securityTokens, acls);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    appContext.setApplicationId(applicationId); // ApplicationId

    String queue = conf.get(AngelConf.ANGEL_QUEUE, YarnConfiguration.DEFAULT_QUEUE_NAME);
    appContext.setQueue(queue); // Queue name
    LOG.info("ApplicationSubmissionContext Queuename :  " + queue);
    appContext.setApplicationName(conf.get(AngelConf.ANGEL_JOB_NAME,
        AngelConf.DEFAULT_ANGEL_JOB_NAME));
    appContext.setCancelTokensWhenComplete(conf.getBoolean(
        AngelConf.JOB_CANCEL_DELEGATION_TOKEN, true));
    appContext.setAMContainerSpec(amContainer); // AM Container
    appContext.setMaxAppAttempts(conf.getInt(AngelConf.ANGEL_AM_MAX_ATTEMPTS,
        AngelConf.DEFAULT_ANGEL_AM_MAX_ATTEMPTS));
    appContext.setResource(capability);
    appContext.setApplicationType(AngelConf.ANGEL_APPLICATION_TYPE);
    return appContext;
  }

  private String masterJVMOptions(Configuration conf) {
    int masterMemoryMB = conf.getInt(AngelConf.ANGEL_AM_MEMORY_GB, AngelConf.DEFAULT_ANGEL_AM_MEMORY_GB) * 1024;
    if(masterMemoryMB < 1024) {
      masterMemoryMB = 1024;
    }

    int heapMax = masterMemoryMB - 512;
    return new StringBuilder().append(" -Xmx").append(heapMax).append("M").append(" -Xms")
      .append(heapMax).append("M").append(" -XX:PermSize=100M -XX:MaxPermSize=200M")
      .append(" -XX:+PrintGCDateStamps").append(" -XX:+PrintGCDetails")
      .append(" -XX:+PrintCommandLineFlags").append(" -XX:+PrintTenuringDistribution")
      .append(" -XX:+PrintAdaptiveSizePolicy").append(" -Xloggc:<LOG_DIR>/gc.log").toString();
  }

  private LocalResource createApplicationResource(FileContext fs, Path p, LocalResourceType type)
      throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs.getDefaultFileSystem().resolvePath(
        rsrcStat.getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }

  @Override
  protected void updateMaster(int maxWaitSeconds) throws Exception  {
    String host = null;
    int port = -1;
    int tryTime = 0;
    TConnection connection = TConnectionManager.getConnection(conf);
    while (tryTime < maxWaitSeconds) {
      ApplicationReport appMaster = yarnClient.getApplicationReport(appId);
      String diagnostics =
          (appMaster == null ? "application report is null" : appMaster.getDiagnostics());
      if (appMaster == null || appMaster.getYarnApplicationState() == YarnApplicationState.FAILED
          || appMaster.getYarnApplicationState() == YarnApplicationState.KILLED) {
        throw new IOException("Failed to run job : " + diagnostics);
      }

      if(appMaster.getYarnApplicationState() == YarnApplicationState.FINISHED) {
        LOG.info("application is finished!!");
        master = null;
        return;
      }

      host = appMaster.getHost();
      port = appMaster.getRpcPort();
      if (host == null || "".equals(host)) {
        LOG.info("AM not assigned to Job. Waiting to get the AM ...");
        Thread.sleep(1000);
        tryTime++;
      } else if (UNAVAILABLE.equals(host)) {
        Thread.sleep(1000);
        tryTime++;
      } else {
        LOG.info("appMaster getTrackingUrl = " + appMaster.getTrackingUrl().replace("proxy", "cluster/app"));
        LOG.info("master host=" + host + ", port=" + port);
        try {
          masterLocation = new Location(host, port);
          LOG.info("start to create rpc client to am");         
          master = connection.getMasterService(masterLocation.getIp(), masterLocation.getPort());
          startHeartbeat();
        } catch (ServiceException e) {
          LOG.error("Register to Master failed, ", e);
          Thread.sleep(1000);
          tryTime++;
          continue;
        }
        break;
      }
    }

    if(tryTime >= maxWaitSeconds && masterLocation == null) {
      throw new IOException("wait for master location timeout");
    }
  }

  @Override
  protected String getAppId() {
    return appId.toString();
  }

  @Override protected void printWorkerLogUrl(WorkerId workerId)  {
    try {
      while(true){
        ClientMasterServiceProtos.GetWorkerLogDirResponse response = master.getWorkerLogDir(null,
          ClientMasterServiceProtos.GetWorkerLogDirRequest.newBuilder().setWorkerId(ProtobufUtil.convertToIdProto(workerId)).build());
        if(response.getLogDir().isEmpty()){
          Thread.sleep(1000);
          continue;
        }

        LOG.info(workerId + " log file=" + response.getLogDir());
        break;
      }
    } catch (Exception e) {
      LOG.error("get " + workerId + " log file failed ", e);
    }
  }
}
