package com.tencent.angel.client.kubernetes;

import com.tencent.angel.kubernetesmanager.deploy.submit.KubernetesClientApplication;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;

public final class KubernetesSubmissionEntry {
  private final KubernetesClientApplication application = new KubernetesClientApplication();

  public void run(String xml) {
    Configuration conf = new Configuration();
    conf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    application.run(conf);
  }

  public String getAngelMasterPodIp() { return application.getAngelMasterPodIp(); }
  public void deleteMasterPod() { application.deleteMasterPod(); }
}
