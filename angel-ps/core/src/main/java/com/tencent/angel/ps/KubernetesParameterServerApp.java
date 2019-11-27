package com.tencent.angel.ps;

import com.tencent.angel.RunningMode;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.kubernetesmanager.deploy.config.Constants;
import com.tencent.angel.utils.ConfUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * kubernetes parameter server entry.
 */

public class KubernetesParameterServerApp {
    private static final Log LOG = LogFactory.getLog(KubernetesParameterServerApp.class);
    public static void main(String[] args) throws IOException {
        LOG.info("Starting Parameter Server...");
        int serverIndex = Integer.valueOf(System.getenv(Constants.ENV_EXECUTOR_ID()));
        String appMasterHost = System.getenv(Constants.ENV_MASTER_BIND_ADDRESS());
        int appMasterPort = Integer.valueOf(System.getenv(Constants.ENV_MASTER_BIND_PORT()));
        int attemptIndex = Integer.valueOf(System.getenv(Constants.ENV_EXECUTOR_ATTEMPT_ID()));

        Configuration conf = new Configuration();
        ConfUtils.addResourceProperties(conf, Constants.ANGEL_CONF_PATH());
        String runningMode =
                conf.get(AngelConf.ANGEL_RUNNING_MODE, AngelConf.DEFAULT_ANGEL_RUNNING_MODE);
        if (runningMode.equals(RunningMode.ANGEL_PS_WORKER.toString())) {
            conf.set(AngelConf.ANGEL_TASK_ACTUAL_NUM, System.getenv(Constants.ENV_ANGEL_TASK_NUMBER()));
        }

        final ParameterServer psServer =
                new ParameterServer(serverIndex, attemptIndex, appMasterHost, appMasterPort, conf);
        try {
            psServer.initialize();
            psServer.start();
        } catch (Throwable x) {
            LOG.fatal("Start PS failed ", x);
            psServer.failed(x.getMessage());
        }
        LOG.info("Starting Parameter Server successfully.");
    }
}
