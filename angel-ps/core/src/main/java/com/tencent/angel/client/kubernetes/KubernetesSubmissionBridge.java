package com.tencent.angel.client.kubernetes;

import com.tencent.angel.kubernetesmanager.deploy.submit.KubernetesClientApplication;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public final class KubernetesSubmissionBridge {
  public static final String CLIENT_CLASSPATH = "angel.kubernetes.client.classpath";
  private final KubernetesClientApplication direct;
  private final Object isolated;
  private final URLClassLoader loader;

  public KubernetesSubmissionBridge(Configuration conf) throws Exception {
    String classpath = conf.get(CLIENT_CLASSPATH, "");
    if (classpath.trim().isEmpty()) {
      direct = new KubernetesClientApplication();
      isolated = null;
      loader = null;
    } else {
      direct = null;
      List<URL> urls = new ArrayList<>();
      for (String entry : classpath.split(File.pathSeparator)) {
        if (entry.endsWith("*")) {
          File directory = new File(entry.substring(0, entry.length() - 1));
          File[] jars = directory.listFiles((dir, name) -> name.endsWith(".jar")
              && !name.startsWith("hadoop-client-api-") && !name.startsWith("hadoop-client-runtime-"));
          if (jars == null) throw new IllegalArgumentException("Invalid Kubernetes client classpath: " + entry);
          java.util.Arrays.sort(jars);
          for (File jar : jars) urls.add(jar.toURI().toURL());
        } else {
          File path = new File(entry);
          if (!path.exists()) throw new IllegalArgumentException("Missing Kubernetes client classpath: " + entry);
          urls.add(path.toURI().toURL());
        }
      }
      loader = new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getSystemClassLoader().getParent());
      isolated = loader.loadClass("com.tencent.angel.client.kubernetes.KubernetesSubmissionEntry")
          .getDeclaredConstructor().newInstance();
    }
  }

  public void run(Configuration conf) throws Exception {
    if (direct != null) direct.run(conf);
    else {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      conf.writeXml(output);
      invoke("run", new Class<?>[]{String.class}, new String(output.toByteArray(), StandardCharsets.UTF_8));
    }
  }

  public String getAngelMasterPodIp() throws Exception {
    return direct != null ? direct.getAngelMasterPodIp() : (String) invoke("getAngelMasterPodIp", new Class<?>[0]);
  }

  public void deleteMasterPod() {
    if (direct != null) direct.deleteMasterPod();
    else {
      try { invoke("deleteMasterPod", new Class<?>[0]); }
      catch (Exception failure) { throw new IllegalStateException("Kubernetes submission cleanup failed", failure); }
    }
  }

  private Object invoke(String method, Class<?>[] types, Object... arguments) throws Exception {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    try {
      thread.setContextClassLoader(loader);
      return isolated.getClass().getMethod(method, types).invoke(isolated, arguments);
    } catch (InvocationTargetException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception) throw (Exception) cause;
      if (cause instanceof Error) throw (Error) cause;
      throw failure;
    } finally { thread.setContextClassLoader(previous); }
  }
}
