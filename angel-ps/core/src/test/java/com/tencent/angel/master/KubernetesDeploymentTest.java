package com.tencent.angel.master;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.client.kubernetes.AngelKubernetesClient;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.deploy.ContainerAllocatorEventType;
import com.tencent.angel.master.deploy.ContainerAllocatorFactory;
import com.tencent.angel.master.deploy.ContainerLauncher;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.deploy.ContainerLauncherEventType;
import com.tencent.angel.master.deploy.ContainerLauncherFactory;
import com.tencent.angel.master.deploy.DeployEventUtils;
import com.tencent.angel.master.deploy.kubernetes.KubernetesContainerAllocator;
import com.tencent.angel.master.deploy.kubernetes.KubernetesContainerLauncher;
import com.tencent.angel.master.ps.ParameterServerManager;
import com.tencent.angel.master.ps.attempt.PSAttempt;
import com.tencent.angel.master.ps.attempt.PSAttemptContainerAssignedEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.master.worker.attempt.WorkerAttemptContainerAssignedEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KubernetesDeploymentTest {
  private AMContext context() {
    AMContext context = mock(AMContext.class);
    when(context.getConf()).thenReturn(new Configuration(false));
    when(context.getApplicationName()).thenReturn("kubernetes-test");
    when(context.getDeployMode()).thenReturn(AngelDeployMode.KUBERNETES);
    when(context.getEventHandler()).thenReturn(mock(EventHandler.class));
    return context;
  }

  @Test
  public void factoriesSelectKubernetes() {
    Configuration conf = new Configuration(false);
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "KUBERNETES");
    assertTrue(AngelClientFactory.get(conf) instanceof AngelKubernetesClient);
    AMContext context = context();
    ContainerAllocator allocator = ContainerAllocatorFactory.create(AngelDeployMode.KUBERNETES, context);
    ContainerLauncher launcher = ContainerLauncherFactory.create(AngelDeployMode.KUBERNETES, context);
    assertTrue(allocator instanceof KubernetesContainerAllocator);
    assertTrue(launcher instanceof KubernetesContainerLauncher);
  }

  @Test
  public void psEventsAssignLaunchAndRemoveOnlyTheAttempt() {
    AMContext context = context();
    ParameterServerManager manager = mock(ParameterServerManager.class);
    LinkedBlockingDeque<PSAttemptId> queue = new LinkedBlockingDeque<>();
    when(manager.getPsAttemptIdBlockingQueue()).thenReturn(queue);
    when(context.getParameterServerManager()).thenReturn(manager);
    KubernetesContainerAllocator allocator = new KubernetesContainerAllocator(context);
    when(context.getContainerAllocator()).thenReturn(allocator);
    KubernetesContainerLauncher launcher = new KubernetesContainerLauncher(context);
    PSAttempt attempt = new PSAttempt(new ParameterServerId(2), 1, context);
    PSAttemptId replacement = new PSAttemptId(new ParameterServerId(2), 2);
    queue.add(attempt.getId());
    queue.add(replacement);
    allocator.handle(DeployEventUtils.createContainerAllocatorEvent(AngelDeployMode.KUBERNETES, attempt));
    ArgumentCaptor<PSAttemptContainerAssignedEvent> assigned =
        ArgumentCaptor.forClass(PSAttemptContainerAssignedEvent.class);
    verify(context.getEventHandler()).handle(assigned.capture());
    assertEquals(PSAttemptEventType.PA_CONTAINER_ASSIGNED, assigned.getValue().getType());
    launcher.handle(DeployEventUtils.createContainerLaunchEvent(AngelDeployMode.KUBERNETES, attempt));
    ArgumentCaptor<PSAttemptEvent> launched = ArgumentCaptor.forClass(PSAttemptEvent.class);
    verify(context.getEventHandler(), times(2)).handle(launched.capture());
    assertEquals(PSAttemptEventType.PA_CONTAINER_LAUNCHED, launched.getValue().getType());
    allocator.handle(DeployEventUtils.createContainerDeAllocatorEvent(AngelDeployMode.KUBERNETES, attempt));
    assertFalse(queue.contains(attempt.getId()));
    assertTrue(queue.contains(replacement));
  }

  @Test
  public void workerEventsDoNotRequireYarnTokens() {
    AMContext context = context();
    WorkerManager manager = mock(WorkerManager.class);
    LinkedBlockingDeque<WorkerAttemptId> queue = new LinkedBlockingDeque<>();
    when(manager.getWorkerAttemptIdBlockingQueue()).thenReturn(queue);
    when(context.getWorkerManager()).thenReturn(manager);
    KubernetesContainerAllocator allocator = new KubernetesContainerAllocator(context);
    when(context.getContainerAllocator()).thenReturn(allocator);
    KubernetesContainerLauncher launcher = new KubernetesContainerLauncher(context);
    WorkerAttemptId id = new WorkerAttemptId(new WorkerId(new WorkerGroupId(1), 1), 0);
    queue.add(id);
    allocator.handle(new ContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_REQ, id) {});
    ArgumentCaptor<WorkerAttemptContainerAssignedEvent> assigned =
        ArgumentCaptor.forClass(WorkerAttemptContainerAssignedEvent.class);
    verify(context.getEventHandler()).handle(assigned.capture());
    assertEquals(WorkerAttemptEventType.CONTAINER_ASSIGN, assigned.getValue().getType());
    launcher.handle(new ContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_LAUNCH, id) {});
    ArgumentCaptor<WorkerAttemptEvent> launched = ArgumentCaptor.forClass(WorkerAttemptEvent.class);
    verify(context.getEventHandler(), times(2)).handle(launched.capture());
    assertEquals(WorkerAttemptEventType.CONTAINER_LAUNCHED, launched.getValue().getType());
    launcher.handle(new ContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_CLEANUP, id) {});
    assertTrue(queue.isEmpty());
  }

  @Test
  public void masterLoadsPodEnvironmentAndProperties() throws Exception {
    Path file = Files.createTempFile("angel-kubernetes-", ".properties");
    try {
      Properties properties = new Properties();
      properties.setProperty(AngelConf.ANGEL_KUBERNETES_APP_ID, "application_123456_0001");
      properties.setProperty(AngelConf.ANGEL_KUBERNETES_MASTER_PORT, "19078");
      try (OutputStream output = Files.newOutputStream(file)) {
        properties.store(output, null);
      }
      Map<String, String> environment = new HashMap<>();
      environment.put("ANGEL_MASTER_BIND_ADDRESS", "10.0.0.1");
      environment.put("ANGEL_MASTER_POD_NAME", "test-master");
      environment.put("ANGEL_EXECUTOR_POD_NAME_PREFIX", "test");
      Configuration conf = AngelKubernetesApplicationMaster.loadConfiguration(file.toString(), environment);
      assertEquals("KUBERNETES", conf.get(AngelConf.ANGEL_DEPLOY_MODE));
      assertEquals("10.0.0.1", conf.get(AngelConf.ANGEL_KUBERNETES_MASTER_POD_IP));
      assertEquals(19078, conf.getInt(AngelConf.ANGEL_KUBERNETES_MASTER_PORT, -1));
      AngelKubernetesApplicationMaster master = new AngelKubernetesApplicationMaster(conf, "test");
      assertEquals(AngelDeployMode.KUBERNETES, master.getAppContext().getDeployMode());
      assertEquals("application_123456_0001", master.getAppContext().getApplicationId().toString());
      environment.remove("ANGEL_MASTER_POD_NAME");
      try {
        AngelKubernetesApplicationMaster.loadConfiguration(file.toString(), environment);
        fail("Missing pod environment must be rejected");
      } catch (IllegalArgumentException expected) {
        assertTrue(expected.getMessage().contains("ANGEL_MASTER_POD_NAME"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
