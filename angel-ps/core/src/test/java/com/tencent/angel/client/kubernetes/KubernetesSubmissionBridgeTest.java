package com.tencent.angel.client.kubernetes;

import java.io.File;
import java.net.URLClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class KubernetesSubmissionBridgeTest {
  @Test
  public void plainClientDoesNotRequireIsolatedClasspath() throws Exception {
    Configuration conf = new Configuration(false);
    KubernetesSubmissionBridge bridge = new KubernetesSubmissionBridge(conf);
    bridge.deleteMasterPod();
  }

  @Test
  public void invalidClasspathFailsWithoutChangingThreadLoader() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(KubernetesSubmissionBridge.CLIENT_CLASSPATH,
        new File("target/missing-kubernetes-libs/*").getAbsolutePath());
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try {
      new KubernetesSubmissionBridge(conf);
      fail("Missing classpath must be reported");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Invalid Kubernetes client classpath"));
    }
    assertSame(previous, Thread.currentThread().getContextClassLoader());
  }

  @Test
  public void isolatedEntryUsesStringBoundary() throws Exception {
    Configuration conf = new Configuration(false);
    String classpath = System.getProperty("surefire.test.class.path",
        System.getProperty("java.class.path"));
    conf.set(KubernetesSubmissionBridge.CLIENT_CLASSPATH, classpath);
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    KubernetesSubmissionBridge bridge = new KubernetesSubmissionBridge(conf);
    bridge.deleteMasterPod();
    assertSame(previous, Thread.currentThread().getContextClassLoader());
  }
}
