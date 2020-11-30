package com.tencent.angel.kubernetesmanager.deploy.features

import com.tencent.angel.kubernetesmanager.deploy.config.{AngelPod, KubernetesConf, KubernetesRoleSpecificConf}
import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, HasMetadata}
import scala.collection.JavaConverters._

private[angel] class EnvSecretsFeatureStep(
    kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: AngelPod): AngelPod = {
    val addedEnvSecrets = kubernetesConf
      .roleSecretEnvNamesToKeyRefs
      .map{ case (envName, keyRef) =>
        // Keyref parts
        val keyRefParts = keyRef.split(":")
        require(keyRefParts.size == 2, "SecretKeyRef must be in the form name:key.")
        val name = keyRefParts(0)
        val key = keyRefParts(1)
        new EnvVarBuilder()
          .withName(envName)
          .withNewValueFrom()
          .withNewSecretKeyRef()
          .withKey(key)
          .withName(name)
          .endSecretKeyRef()
          .endValueFrom()
          .build()
      }

    val containerWithEnvVars = new ContainerBuilder(pod.container)
      .addAllToEnv(addedEnvSecrets.toSeq.asJava)
      .build()
    AngelPod(pod.pod, containerWithEnvVars)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
