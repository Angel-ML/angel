package com.tencent.angel.master;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.kubernetesmanager.deploy.config.Constants;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.location.LocationManager;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.*;
import com.tencent.angel.master.client.ClientManager;
import com.tencent.angel.master.data.DataSpliter;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.kubernetesmanager.scheduler.KubernetesClusterManager;
import com.tencent.angel.master.matrix.committer.AMModelLoader;
import com.tencent.angel.master.matrix.committer.AMModelSaver;
import com.tencent.angel.master.matrixmeta.AMMatrixMetaManager;
import com.tencent.angel.master.metrics.MetricsEventType;
import com.tencent.angel.master.metrics.MetricsService;
import com.tencent.angel.master.oplog.AppStateStorage;
import com.tencent.angel.master.ps.ParameterServerManager;
import com.tencent.angel.master.ps.ParameterServerManagerEventType;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.ps.ps.AMParameterServer;
import com.tencent.angel.master.ps.ps.AMParameterServerEvent;
import com.tencent.angel.master.ps.ps.AMParameterServerEventType;
import com.tencent.angel.master.psagent.*;
import com.tencent.angel.master.slowcheck.SlowChecker;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.plugin.AngelServiceLoader;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.webapp.WebApp;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Angel application master. It contains service modules: worker manager, parameter server manager,
 * container allocator, container launcher, task manager and event handler.
 */
public class AngelKubernetesApplicationMaster extends CompositeService {

    private static final Log LOG = LogFactory.getLog(AngelKubernetesApplicationMaster.class);
    public static final int SHUTDOWN_HOOK_PRIORITY = 30;

    /**
     * application name
     */
    private final String appName;

    private static Location masterLocation;

    /**
     * application configuration
     */
    private final Configuration conf;

    /**
     * app start time
     */
    private final long startTime;

    /**
     * application running context, it is used to share information between all service module
     */
    private final AMContext appContext;

    /**
     * system clock
     */
    private final SystemClock clock;

    /**
     * event dispatcher
     */
    private Dispatcher dispatcher;

    /**
     * parameter server manager
     */
    private ParameterServerManager psManager;

    /**
     * angel application master service, it is used to response RPC request from client, workers and
     * parameter servers
     */
    private volatile MasterService masterService;

    /**
     * matrix meta manager
     */
    private AMMatrixMetaManager matrixMetaManager;

    /**
     * parameter server location manager
     */
    private LocationManager locationManager;

    /**
     * worker manager
     */
    private WorkerManager workerManager;

    /**
     * it use to split train data
     */
    private DataSpliter dataSpliter;

    /**
     * angel application master state storage
     */
    private AppStateStorage appStateStorage;

    /**
     * angel application state
     */
    private final App angelApp;

    /**
     * a web service for http access
     */
    private WebApp webApp;

    /**
     * psagent manager
     */
    private PSAgentManager psAgentManager;

    /**
     * identifies whether the temporary resource is cleared
     */
    private boolean isCleared;

    /**
     * task manager
     */
    private AMTaskManager taskManager;

    /**
     * Algorithm indexes collector
     */
    private MetricsService algoMetricsService;

    private final Lock lock;

    /**
     * Angel Client manager
     */
    private ClientManager clientManager;

    /**
     * Heartbeat monitor
     */
    private HeartbeatMonitor hbMonitor;

    /**
     * Model saver
     */
    private AMModelSaver modelSaver;

    /**
     * Model loader
     */
    private AMModelLoader modelLoader;

    private KubernetesClusterManager k8sClusterManager;

    public AngelKubernetesApplicationMaster(Configuration conf, String appName) {
        super(AngelApplicationMaster.class.getName());
        this.conf = conf;
        this.appName = appName;

        this.clock = new SystemClock();
        this.startTime = clock.getTime();
        this.isCleared = false;

        appContext = new RunningAppContext(conf);
        angelApp = new App(appContext);
        lock = new ReentrantLock();
    }



    /**
     * running application master context
     */
    public class RunningAppContext implements AMContext {

        public RunningAppContext(Configuration config) {

        }

        @Override public ApplicationAttemptId getApplicationAttemptId() {
            return null;
        }

        @Override public MasterService getMasterService() {
            return masterService;
        }

        @Override public ApplicationId getApplicationId() {
            return ConverterUtils.toApplicationId(conf.get(AngelConf.ANGEL_KUBERNETES_APP_ID));
        }

        @Override public String getApplicationName() {
            return appName;
        }

        @Override public long getStartTime() {
            return startTime;
        }

        @SuppressWarnings("rawtypes") @Override public EventHandler getEventHandler() {
            return dispatcher.getEventHandler();
        }

        @Override public String getUser() {
            return conf.get(AngelConf.USER_NAME);
        }

        @Override public Clock getClock() {
            return clock;
        }

        @Override public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
            return null;
        }

        @Override public Credentials getCredentials() {
            return null;
        }

        @Override public ContainerAllocator getContainerAllocator() {
            return null;
        }

        @Override public ParameterServerManager getParameterServerManager() {
            return psManager;
        }

        @Override public Dispatcher getDispatcher() {
            return dispatcher;
        }

        @Override public App getApp() {
            return angelApp;
        }

        @Override public Configuration getConf() {
            return conf;
        }

        @Override public WebApp getWebApp() {
            return webApp;
        }

        @Override public AMMatrixMetaManager getMatrixMetaManager() {
            return matrixMetaManager;
        }

        @Override public LocationManager getLocationManager() {
            return locationManager;
        }

        @Override public RunningMode getRunningMode() {
            String mode = conf.get(AngelConf.ANGEL_RUNNING_MODE, AngelConf.DEFAULT_ANGEL_RUNNING_MODE);
            if (mode.equals(RunningMode.ANGEL_PS.toString())) {
                return RunningMode.ANGEL_PS;
            } else {
                return RunningMode.ANGEL_PS_WORKER;
            }
        }

        @Override public PSAgentManager getPSAgentManager() {
            return psAgentManager;
        }

        @Override public WorkerManager getWorkerManager() {
            return workerManager;
        }

        @Override public DataSpliter getDataSpliter() {
            return dataSpliter;
        }

        @Override public int getTotalIterationNum() {
            return conf.getInt("ml.epoch.num", AngelConf.DEFAULT_ANGEL_TASK_ITERATION_NUMBER);
        }

        @Override public AMTaskManager getTaskManager() {
            return taskManager;
        }

        @Override public MetricsService getAlgoMetricsService() {
            return algoMetricsService;
        }

        @Override public int getPSReplicationNum() {
            return conf.getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
                    AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);
        }

        @Override public ClientManager getClientManager() {
            return clientManager;
        }

        @Override public int getYarnNMWebPort() {
            return 0;
        }

        @Override public AMModelSaver getModelSaver() {
            return modelSaver;
        }

        @Override public AMModelLoader getModelLoader() {
            return modelLoader;
        }

        @Override public int getAMAttemptTime() {
            return conf
                    .getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
        }

        @Override public AppStateStorage getAppStateStorage() {
            return appStateStorage;
        }

        @Override public boolean needClear() {
            return !getApp().isShouldRetry() || angelApp.isSuccess();

        }

        @Override public AngelDeployMode getDeployMode() {
            String mode = conf.get(AngelConf.ANGEL_DEPLOY_MODE, AngelConf.DEFAULT_ANGEL_DEPLOY_MODE);
            if (mode.equals(AngelDeployMode.LOCAL.toString())) {
                return AngelDeployMode.LOCAL;
            } else if (mode.equals(AngelDeployMode.KUBERNETES.toString())) {
                return AngelDeployMode.KUBERNETES;
            } else {
                return AngelDeployMode.YARN;
            }
        }
    }

    public void clear() throws IOException {
        boolean deleteSubmitDir = appContext.getConf()
                .getBoolean(AngelConf.ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE,
                        AngelConf.DEFAULT_ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE);
        if (deleteSubmitDir) {
            cleanupStagingDir();
        }

        cleanTmpOutputDir();
    }

    private void cleanTmpOutputDir() {
        Configuration conf = appContext.getConf();
        String tmpOutDir = conf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
        if (tmpOutDir == null) {
            return;
        }

        try {
            LOG.info("Deleting tmp output directory " + tmpOutDir);
            Path tmpOutPath = new Path(tmpOutDir);
            FileSystem fs = tmpOutPath.getFileSystem(conf);
            fs.delete(tmpOutPath, true);
        } catch (IOException io) {
            LOG.error("Failed to cleanup staging dir " + tmpOutDir, io);
        }
    }

    private void cleanupStagingDir() throws IOException {
        Configuration conf = appContext.getConf();
        String stagingDir = conf.get(AngelConf.ANGEL_JOB_DIR);
        if (stagingDir == null) {
            LOG.warn("App Staging directory is null");
            return;
        }

        try {
            Path stagingDirPath = new Path(stagingDir);
            FileSystem fs = stagingDirPath.getFileSystem(conf);
            LOG.info("Deleting staging directory " + FileSystem.getDefaultUri(conf) + " " + stagingDir);
            fs.delete(stagingDirPath, true);
        } catch (IOException io) {
            LOG.error("Failed to cleanup staging dir " + stagingDir, io);
        }
    }

    @Override public void serviceStop() throws Exception {
        super.serviceStop();
        AngelServiceLoader.stopService();
    }

    /**
     * stop all services of angel application master and clear tmp directory
     */
    public void shutDownJob() {
        try {
            lock.lock();
            if (isCleared) {
                return;
            }

            // stop all services
            LOG.info("Calling stop for all the services");
            AngelKubernetesApplicationMaster.this.stop();

            //k8sClusterManager.stop();

            // 1.write application state to file so that the client can get the state of the application
            // if master exit
            // 2.clear tmp and staging directory
            if (appContext.needClear()) {
                LOG.info("start to write app state to file and clear tmp directory");
                writeAppState();
                clear();
            }

            // waiting for client to get application state
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOG.warn("ShutDownjob error ", e);
            }

            // stop the RPC server
            masterService.stop();
        } catch (Throwable t) {
            LOG.warn("Graceful stop failed ", t);
        } finally {
            isCleared = true;
            lock.unlock();
        }

        LOG.info("Exiting Angel AppMaster..GoodBye!");

        exit(0);
    }

    private void exit(int code) {
        AngelDeployMode deployMode = appContext.getDeployMode();
        if (deployMode == AngelDeployMode.YARN) {
            System.exit(code);
        }
    }

    private void writeAppState() throws IllegalArgumentException, IOException {
        String interalStatePath = "/tmp/state";
                //appContext.getConf().get(AngelConf.ANGEL_APP_SERILIZE_STATE_FILE);

        LOG.info("start to write app state to file " + interalStatePath);

        if (interalStatePath == null) {
            LOG.error("can not find app state serilize file, exit");
            return;
        }

        Path stateFilePath = new Path(interalStatePath);
        FileSystem fs = stateFilePath.getFileSystem(appContext.getConf());
        if (fs.exists(stateFilePath)) {
            fs.delete(stateFilePath, false);
        }

        FSDataOutputStream out = fs.create(stateFilePath);
        appContext.getApp().serilize(out);
        out.flush();
        out.close();

        LOG.info("write app state over");
    }

    @SuppressWarnings("resource") public static void main(String[] args) {
        AngelKubernetesAppMasterShutdownHook hook = null;
        try {
            Configuration conf = new Configuration();
            for (Entry<String, String> confTuple : System.getenv().entrySet()) {
                if (confTuple.getKey().startsWith("angel.")) {
                    conf.set(confTuple.getKey(), confTuple.getValue());
                }
            }
            conf.set(AngelConf.ANGEL_KUBERNETES_MASTER_POD_IP,
                    System.getenv(Constants.ENV_MASTER_BIND_ADDRESS()));
            conf.set(AngelConf.ANGEL_KUBERNETES_MASTER_POD_NAME,
                    System.getenv(Constants.ENV_MASTER_POD_NAME()));
            conf.set(AngelConf.ANGEL_KUBERNETES_EXECUTOR_POD_NAME_PREFIX,
                    System.getenv(Constants.ENV_EXECUTOR_POD_NAME_PREFIX()));
            conf.setBoolean("fs.automatic.close", false);

            String appName = conf.get(AngelConf.ANGEL_JOB_NAME);
            LOG.info("app name=" + appName);
            final AngelKubernetesApplicationMaster appMaster =
                    new AngelKubernetesApplicationMaster(conf, appName);

            // add a shutdown hook
            hook = new AngelKubernetesAppMasterShutdownHook(appMaster);
            ShutdownHookManager.get().addShutdownHook(hook, SHUTDOWN_HOOK_PRIORITY);
            appMaster.initAndStart();
        } catch (Throwable t) {
            LOG.fatal("Error starting AppMaster", t);
            if (hook != null) {
                ShutdownHookManager.get().removeShutdownHook(hook);
            }
            System.exit(1);
        }
    }

    /**
     * init and start all service modules for angel applicaiton master.
     */
    public void initAndStart() throws Exception {
        addIfService(angelApp);

        // init app state storage
        String tmpOutPath = conf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
        Path appStatePath = new Path(tmpOutPath, "app");
        LOG.info("app state output path = " + appStatePath.toUri().toString());
        FileSystem fs = appStatePath.getFileSystem(conf);
        appStateStorage = new AppStateStorage(appContext, appStatePath.toUri().toString(), fs);
        addIfService(appStateStorage);
        LOG.info("build app state storage success");

        // init event dispacher
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);
        LOG.info("build event dispacher");

        // init location manager
        locationManager = new LocationManager();

        // init a rpc service
        masterService = new MasterService(appContext);
        LOG.info("build master service success");

        // recover matrix meta if needed
        recoverMatrixMeta();

        // recover ps attempt information if need
        Map<ParameterServerId, Integer> psIdToAttemptIndexMap = recoverPSAttemptIndex();
        if (psIdToAttemptIndexMap == null) {
            LOG.info("recoverPSAttemptIndex return is null");
        } else {
            for (Entry<ParameterServerId, Integer> entry : psIdToAttemptIndexMap.entrySet()) {
                LOG.info("psId=" + entry.getKey() + ",attemptIndex=" + entry.getValue());
            }
        }

        // Init Client manager
        clientManager = new ClientManager(appContext);
        addIfService(clientManager);

        // Init PS Client manager
        psAgentManager = new PSAgentManager(appContext);
        addIfService(psAgentManager);

        // init parameter server manager
        psManager = new ParameterServerManager(appContext, psIdToAttemptIndexMap);
        addIfService(psManager);
        psManager.init();
        List<ParameterServerId> psIds = new ArrayList<>(psManager.getParameterServerMap().keySet());
        Collections.sort(psIds, new Comparator<ParameterServerId>() {
            @Override public int compare(ParameterServerId s1, ParameterServerId s2) {
                return s1.getIndex() - s2.getIndex();
            }
        });
        locationManager.setPsIds(psIds.toArray(new ParameterServerId[0]));

        dispatcher.register(ParameterServerManagerEventType.class, psManager);
        dispatcher.register(AMParameterServerEventType.class, new ParameterServerEventHandler());
        dispatcher.register(PSAttemptEventType.class, new PSAttemptEventDispatcher());
        LOG.info("build PSManager success");

        // recover task information if needed
        recoverTaskState();

        // register slow worker/ps checker
        addIfService(new SlowChecker(appContext));

        algoMetricsService = new MetricsService(appContext);
        addIfService(algoMetricsService);
        dispatcher.register(MetricsEventType.class, algoMetricsService);

        // register app manager event and finish event
        dispatcher.register(AppEventType.class, angelApp);
        dispatcher.register(AppFinishEventType.class, new AppFinishEventHandler());

        // Init model saver & loader
        modelSaver = new AMModelSaver(appContext);
        addIfService(modelSaver);
        modelLoader = new AMModelLoader(appContext);
        addIfService(modelLoader);

        hbMonitor = new HeartbeatMonitor(appContext);
        addIfService(hbMonitor);

        masterService.init(conf);
        super.init(conf);

        masterService.start();
        locationManager.setMasterLocation(masterService.getLocation());

        super.serviceStart();
        k8sClusterManager = new KubernetesClusterManager();
        String host = conf.get(AngelConf.ANGEL_KUBERNETES_MASTER_POD_IP);
        int port = conf.getInt(AngelConf.ANGEL_KUBERNETES_MASTER_PORT, AngelConf.DEFAULT_ANGEL_KUBERNETES_MASTER_PORT);
        masterLocation = new Location(host, port);
        psManager.startAllPS();
        k8sClusterManager.scheduler(conf);
        AngelServiceLoader.startServiceIfNeed(this, getConfig());
    }

    public static Location getMasterLocation() {
        return masterLocation;
    }


    private void waitForAllPsRegisted() throws InterruptedException {
        while (true) {
            if (locationManager.isAllPsRegisted()) {
                return;
            }
            Thread.sleep(100);
        }
    }

    private void waitForAllMetricsInited() throws InterruptedException {
        while (true) {
            if (matrixMetaManager.isAllMatricesCreated()) {
                return;
            }
            Thread.sleep(100);
        }
    }

    private void recoverMatrixMeta() {
        // load from app state storage first if attempt index great than 1(the master is not the first
        // retry)
        if (1 > 1) {
            try {
                matrixMetaManager = appStateStorage.loadMatrixMeta();
            } catch (Exception e) {
                LOG.error("load matrix meta from file failed.", e);
            }
        }

        // if load failed, just build a new MatrixMetaManager
        if (matrixMetaManager == null) {
            matrixMetaManager = new AMMatrixMetaManager(appContext);
        }
    }

    private Map<ParameterServerId, Integer> recoverPSAttemptIndex() {
        // load ps attempt index from app state storage first if attempt index great than 1(the master
        // is not the first retry)
        Map<ParameterServerId, Integer> psIdToAttemptIndexMap = null;
        if (1 > 1) {
            try {
                psIdToAttemptIndexMap = appStateStorage.loadPSMeta();
            } catch (Exception e) {
                LOG.error("load task meta from file failed.", e);
            }
        }

        return psIdToAttemptIndexMap;
    }

    private void recoverTaskState() throws IOException {
        // load task information from app state storage first if attempt index great than 1(the master
        // is not the first retry)
        if (1 > 1) {
            try {
                taskManager = appStateStorage.loadTaskMeta();
            } catch (Exception e) {
                LOG.error("load task meta from file failed.", e);
            }
        }

        // if load failed, just build a new AMTaskManager
        if (taskManager == null) {
            taskManager = new AMTaskManager();
        }
    }


    private void recoveryDataSplits() throws IOException {
        // load data splits information from app state storage first if attempt index great than 1(the
        // master is not the first retry)
        if (1 > 1) {
            try {
                dataSpliter = appStateStorage.loadDataSplits();
            } catch (Exception e) {
                LOG.error("load split from split file failed.", e);
            }
        }

        // if load failed, we need to recalculate the data splits
        if (dataSpliter == null) {
            try {
                dataSpliter = new DataSpliter(appContext);
                dataSpliter.generateSplits();
                appStateStorage.writeDataSplits(dataSpliter);
            } catch (Exception x) {
                LOG.error("Split failed, error: " + x);
                throw new IOException(x);
            }
        }

        if (dataSpliter.getSplitNum() == 0) {
            throw new IOException("training data directory is empty");
        }
    }

    public AMContext getAppContext() {
        return appContext;
    }

    public String getAppName() {
        return appName;
    }

    public class ParameterServerEventHandler implements EventHandler<AMParameterServerEvent> {

        @Override public void handle(AMParameterServerEvent event) {
            ParameterServerId id = event.getPsId();
            AMParameterServer server = psManager.getParameterServer(id);
            server.handle(event);
        }
    }


    public class PSAttemptEventDispatcher implements EventHandler<PSAttemptEvent> {

        @Override public void handle(PSAttemptEvent event) {
            PSAttemptId attemptId = event.getPSAttemptId();
            ParameterServerId id = attemptId.getPsId();
            psManager.getParameterServer(id).getPSAttempt(attemptId).handle(event);
        }
    }


    public class AppFinishEventHandler implements EventHandler<AppFinishEvent> {
        @Override public void handle(AppFinishEvent event) {
            switch (event.getType()) {
                case INTERNAL_ERROR_FINISH:
                case SUCCESS_FINISH:
                case KILL_FINISH: {
                    new Thread() {
                        @Override public void run() {
                            shutDownJob();
                        }
                    }.start();
                }
            }
        }
    }


    static class AngelKubernetesAppMasterShutdownHook implements Runnable {
        AngelKubernetesApplicationMaster appMaster;

        AngelKubernetesAppMasterShutdownHook(AngelKubernetesApplicationMaster appMaster) {
            this.appMaster = appMaster;
        }

        public void run() {
            LOG.info("AM received a signal. stop the app");
            appMaster.getAppContext().getApp().addDiagnostics("killed");
            appMaster.getAppContext().getApp().forceState(AppState.KILLED);
            appMaster.shutDownJob();
        }
    }
}
