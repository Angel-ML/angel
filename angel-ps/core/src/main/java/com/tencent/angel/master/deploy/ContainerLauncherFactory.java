package com.tencent.angel.master.deploy;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerLauncher;
import com.tencent.angel.master.deploy.embedded.EmbeddedContainerLauncher;
import com.tencent.angel.master.deploy.local.LocalContainerLauncher;
import com.tencent.angel.master.deploy.standalone.StandaloneContainerLauncher;
import com.tencent.angel.master.deploy.yarn.YarnContainerLauncher;

public class ContainerLauncherFactory {

  public static ContainerLauncher create(AngelDeployMode mode, AMContext context) {
    switch (mode) {
      case YARN:
        return new YarnContainerLauncher(context);
      case LOCAL:
        return new LocalContainerLauncher(context);
      case EMBEDDED:
        return new EmbeddedContainerLauncher(context);
      case STANDALONE:
        return new StandaloneContainerLauncher(context);
      case KUBERNETES:
        // TODO:
        throw new UnsupportedOperationException();

      default:
        throw new UnsupportedOperationException("Unsupport deploy mode " + mode);
    }
  }
}
