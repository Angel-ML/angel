package com.tencent.angel.kubernetesmanager.deploy.features
import com.tencent.angel.kubernetesmanager.deploy.config.AngelPod
import io.fabric8.kubernetes.api.model.HasMetadata

/**
  * A collection of functions that together represent a "feature" in pods that are launched for
  * Angel master and executors.
  */
private[angel] trait KubernetesFeatureConfigStep {

  /**
    * Apply modifications on the given pod in accordance to this feature. This can include attaching
    * volumes, adding environment variables, and adding labels/annotations.
    * <p>
    * Note that we should return a AngelPod that keeps all of the properties of the passed AngelPod
    * object. So this is correct:
    * <pre>
    * {@code val configuredPod = new PodBuilder(pod.pod)
   *     .editSpec()
   *     ...
   *     .build()
   *   val configuredContainer = new ContainerBuilder(pod.container)
   *     ...
   *     .build()
   *   AngelPod(configuredPod, configuredContainer)
   *  }
    * </pre>
    * This is incorrect:
    * <pre>
    * {@code val configuredPod = new PodBuilder() // Loses the original state
   *     .editSpec()
   *     ...
   *     .build()
   *   val configuredContainer = new ContainerBuilder() // Loses the original state
   *     ...
   *     .build()
   *   AngelPod(configuredPod, configuredContainer)
   *  }
    * </pre>
    */
  def configurePod(pod: AngelPod): AngelPod

  /**
    * Return any system properties that should be set on the JVM in accordance to this feature.
    */
  def getAdditionalPodSystemProperties(): Map[String, String]

  /**
    * Return any additional Kubernetes resources that should be added to support this feature. Only
    * applicable when creating the angel master.
    */
  def getAdditionalKubernetesResources(): Seq[HasMetadata]
}
