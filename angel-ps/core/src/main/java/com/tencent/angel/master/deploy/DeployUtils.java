package com.tencent.angel.master.deploy;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.conf.AngelConf;
import org.apache.hadoop.conf.Configuration;

public class DeployUtils {

  public static AngelDeployMode getDeployMode(Configuration conf) {
    String mode = conf.get(AngelConf.ANGEL_DEPLOY_MODE, AngelConf.DEFAULT_ANGEL_DEPLOY_MODE);
    if (mode.equals(AngelDeployMode.LOCAL.toString())) {
      return AngelDeployMode.LOCAL;
    } else if (mode.equals(AngelDeployMode.EMBEDDED.toString())) {
      return AngelDeployMode.EMBEDDED;
    } else if (mode.equals(AngelDeployMode.STANDALONE.toString())) {
      return AngelDeployMode.STANDALONE;
    } else if (mode.equals(AngelDeployMode.KUBERNETES.toString())) {
      return AngelDeployMode.KUBERNETES;
    } else {
      return AngelDeployMode.YARN;
    }
  }
}
