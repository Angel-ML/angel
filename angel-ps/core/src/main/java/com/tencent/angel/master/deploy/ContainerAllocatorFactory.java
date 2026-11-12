package com.tencent.angel.master.deploy;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.deploy.embedded.EmbeddedContainerAllocator;
import com.tencent.angel.master.deploy.local.LocalContainerAllocator;
import com.tencent.angel.master.deploy.standalone.StandaloneContainerAllocator;
import com.tencent.angel.master.deploy.yarn.YarnContainerAllocator;

public class ContainerAllocatorFactory {

  public static ContainerAllocator create(AngelDeployMode mode, AMContext context) {
    switch (mode) {
      case YARN:
        return new YarnContainerAllocator(context);
      case LOCAL:
        return new LocalContainerAllocator(context);
      case EMBEDDED:
        return new EmbeddedContainerAllocator(context);
      case STANDALONE:
        return new StandaloneContainerAllocator(context);
      case KUBERNETES:
        return new com.tencent.angel.master.deploy.kubernetes.KubernetesContainerAllocator(context);

      default:
        throw new UnsupportedOperationException("Unsupport deploy mode " + mode);
    }
  }
}
