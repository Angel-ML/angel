package com.tencent.angel.ps;

import com.tencent.angel.kubernetesmanager.deploy.config.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * kubernetes parameter server entry.
 */

public class KubernetesParameterServerApp {
    private static final Log LOG = LogFactory.getLog(KubernetesParameterServerApp.class);
    public static void main(String[] args) {
        System.out.println("Starting Parameter Server");
        int serverIndex = Integer.valueOf(System.getenv(Constants.ENV_EXECUTOR_ID())) - 1;
        String appMasterHost = System.getenv(Constants.ENV_MASTER_BIND_ADDRESS());
        int appMasterPort = Integer.valueOf(System.getenv(Constants.ENV_MASTER_BIND_PORT()));
        int attemptIndex = 0;

        Configuration conf = new Configuration();

        for (Map.Entry<String, String> confTuple : System.getenv().entrySet()) {
            if (confTuple.getKey().startsWith("angel.")) {
                conf.set(confTuple.getKey(), confTuple.getValue());
            }
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
