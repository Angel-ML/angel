package com.tencent.angel.worker;

import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.kubernetesmanager.deploy.config.Constants;
import com.tencent.angel.utils.ConfUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.io.IOException;

/**
 * kubernetes worker entry.
 */
public class KubernetesWorkerApp {
    private static final Log LOG = LogFactory.getLog(KubernetesWorkerApp.class);
    public static void main(String[] args) throws IOException {
        LOG.info("Starting worker...");
        // get configuration from envs
        Configuration conf = new Configuration();
        ConfUtils.addResourceProperties(conf, Constants.ANGEL_CONF_PATH());

        long clusterTimestamp = Long.parseLong(conf.get(AngelConf.ANGEL_KUBERNETES_APP_CLUSTERTIMESTAMP));
        int randomId = Integer.parseInt(conf.get(AngelConf.ANGEL_KUBERNETES_APP_RANDOMID));
        ApplicationId appId = ApplicationId.newInstance(clusterTimestamp, randomId);
        String user = System.getenv(ApplicationConstants.Environment.USER.name());


        int workerGroupIndex = Integer.parseInt(System.getenv(Constants.ENV_EXECUTOR_ID())) - 1;
        int workerIndex = workerGroupIndex;
        int attemptIndex = 0;

        WorkerGroupId workerGroupId = new WorkerGroupId(workerGroupIndex);
        WorkerId workerId = new WorkerId(workerGroupId, workerIndex);
        WorkerAttemptId workerAttemptId = new WorkerAttemptId(workerId, attemptIndex);

        conf.set(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM,
                System.getenv(Constants.ENV_ANGEL_WORKERGROUP_NUMBER()));

        conf.set(AngelConf.ANGEL_TASK_ACTUAL_NUM, System.getenv(Constants.ENV_ANGEL_TASK_NUMBER()));
        conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, System.getenv(Constants.ENV_ANGEL_USER_TASK()));

        LOG.info("actual workergroup number:" + conf.get(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM));
        LOG.info("actual task number:" + conf.get(AngelConf.ANGEL_TASK_ACTUAL_NUM));

        // get master location
        String appMasterHost = System.getenv(Constants.ENV_MASTER_BIND_ADDRESS());
        int appMasterPort = Integer.valueOf(System.getenv(Constants.ENV_MASTER_BIND_PORT()));
        Location masterLocation = new Location(appMasterHost, appMasterPort);
        LOG.info("appMasterHost is " + appMasterHost + ", appMasterPort is " + appMasterPort);
        conf.setBoolean("mapred.mapper.new-api", true);
        Worker worker = new Worker(AngelConf.clone(conf), appId, user, workerAttemptId, masterLocation,
                0, false);

        try {
            worker.initAndStart();
        } catch (Exception e) {
            LOG.fatal("Failed to start worker.", e);
            worker.error(e.getMessage());
        }
    }
}
