package com.tencent.angel.master;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.kubernetesmanager.deploy.config.Constants;
import com.tencent.angel.utils.ConfUtils;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class AngelKubernetesApplicationMaster extends AngelApplicationMaster {
  private static final Log LOG = LogFactory.getLog(AngelKubernetesApplicationMaster.class);

  public AngelKubernetesApplicationMaster(Configuration conf, String appName) {
    this(conf, appName, new Credentials());
  }

  private AngelKubernetesApplicationMaster(Configuration conf, String appName,
      Credentials credentials) {
    super(conf, appName, applicationAttempt(conf), null,
        conf.get(AngelConf.ANGEL_KUBERNETES_MASTER_POD_IP), 0, 0,
        System.currentTimeMillis(), credentials);
  }

  private static ApplicationAttemptId applicationAttempt(Configuration conf) {
    String id = conf.get(AngelConf.ANGEL_KUBERNETES_APP_ID);
    if (id == null || id.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing " + AngelConf.ANGEL_KUBERNETES_APP_ID);
    }
    ApplicationId appId = ConverterUtils.toApplicationId(id);
    return ApplicationAttemptId.newInstance(appId, 1);
  }

  static Configuration loadConfiguration(String propertiesPath, Map<String, String> environment)
      throws Exception {
    Configuration conf = new Configuration();
    ConfUtils.addResourceProperties(conf, propertiesPath);
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "KUBERNETES");
    setFromEnvironment(conf, environment, Constants.ENV_MASTER_BIND_ADDRESS(),
        AngelConf.ANGEL_KUBERNETES_MASTER_POD_IP);
    setFromEnvironment(conf, environment, Constants.ENV_MASTER_POD_NAME(),
        AngelConf.ANGEL_KUBERNETES_MASTER_POD_NAME);
    setFromEnvironment(conf, environment, Constants.ENV_EXECUTOR_POD_NAME_PREFIX(),
        AngelConf.ANGEL_KUBERNETES_EXECUTOR_POD_NAME_PREFIX);
    conf.setBoolean("fs.automatic.close", false);
    conf.setBoolean("mapred.mapper.new-api", true);
    return conf;
  }

  private static void setFromEnvironment(Configuration conf, Map<String, String> environment,
      String variable, String key) {
    String value = environment.get(variable);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing Kubernetes environment variable " + variable);
    }
    conf.set(key, value);
  }

  public static void main(String[] args) {
    AngelAppMasterShutdownHook hook = null;
    try {
      Configuration conf = loadConfiguration(Constants.ANGEL_CONF_PATH(), System.getenv());
      org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
      Credentials credentials = org.apache.hadoop.security.UserGroupInformation
          .getCurrentUser().getCredentials();
      org.apache.hadoop.security.UserGroupInformation user =
          org.apache.hadoop.security.UserGroupInformation.createRemoteUser(
              conf.get(AngelConf.USER_NAME, System.getProperty("user.name")));
      user.addCredentials(credentials);
      final AngelKubernetesApplicationMaster master = new AngelKubernetesApplicationMaster(conf,
          conf.get(AngelConf.ANGEL_JOB_NAME, AngelConf.DEFAULT_ANGEL_JOB_NAME), credentials);
      hook = new AngelAppMasterShutdownHook(master);
      ShutdownHookManager.get().addShutdownHook(hook, 30);
      user.doAs(new java.security.PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          master.initAndStart();
          return null;
        }
      });
    } catch (Throwable failure) {
      LOG.fatal("Error starting Kubernetes AppMaster", failure);
      if (hook != null) {
        ShutdownHookManager.get().removeShutdownHook(hook);
      }
      System.exit(1);
    }
  }
}
