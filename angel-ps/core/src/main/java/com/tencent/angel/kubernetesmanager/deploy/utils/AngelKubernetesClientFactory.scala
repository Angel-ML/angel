package com.tencent.angel.kubernetesmanager.deploy.utils

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.tencent.angel.conf.AngelConf
import io.fabric8.kubernetes.client.utils.HttpClientUtils
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import okhttp3.Dispatcher
import org.apache.hadoop.conf.Configuration

private[angel] object AngelKubernetesClientFactory {

  def createKubernetesClient(
      master: String,
      namespace: Option[String],
      kubernetesAuthConfPrefix: String,
      angelConf: Configuration,
      defaultServiceAccountToken: Option[File],
      defaultServiceAccountCaCert: Option[File]): KubernetesClient = {
    val oauthTokenFileConf = s"$kubernetesAuthConfPrefix." + AngelConf.OAUTH_TOKEN_FILE_CONF_SUFFIX
    val oauthTokenConf = s"$kubernetesAuthConfPrefix." + AngelConf.OAUTH_TOKEN_CONF_SUFFIX
    val oauthTokenFile = Option(angelConf.get(oauthTokenFileConf)).map(new File(_))
      .orElse(defaultServiceAccountToken)
    val oauthTokenValue = Option(angelConf.get(oauthTokenConf))
    KubernetesUtils.requireNandDefined(
      oauthTokenFile,
      oauthTokenValue,
      s"Cannot specify OAuth token through both a file $oauthTokenFileConf and a " +
        s"value $oauthTokenConf.")

    val caCertFile = Option(angelConf.get(s"$kubernetesAuthConfPrefix." + AngelConf.CA_CERT_FILE_CONF_SUFFIX))
      .orElse(defaultServiceAccountCaCert.map(_.getAbsolutePath))
    val clientKeyFile = Option(angelConf.get(s"$kubernetesAuthConfPrefix." + AngelConf.CLIENT_KEY_FILE_CONF_SUFFIX))
    val clientCertFile = Option(angelConf.get(s"$kubernetesAuthConfPrefix." + AngelConf.CLIENT_CERT_FILE_CONF_SUFFIX))
    val dispatcher = new Dispatcher(ThreadUtils.newDaemonCachedThreadPool("kubernetes-dispatcher"))
    val config = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withWebsocketPingInterval(0)
      .withOption(oauthTokenValue) {
        (token, configBuilder) => configBuilder.withOauthToken(token)
      }.withOption(oauthTokenFile) {
      (file, configBuilder) =>
        configBuilder.withOauthToken(Files.toString(file, Charsets.UTF_8))
    }.withOption(caCertFile) {
      (file, configBuilder) => configBuilder.withCaCertFile(file)
    }.withOption(clientKeyFile) {
      (file, configBuilder) => configBuilder.withClientKeyFile(file)
    }.withOption(clientCertFile) {
      (file, configBuilder) => configBuilder.withClientCertFile(file)
    }.withOption(namespace) {
      (ns, configBuilder) => configBuilder.withNamespace(ns)
    }.build()
    val baseHttpClient = HttpClientUtils.createHttpClient(config)
    val httpClientWithCustomDispatcher = baseHttpClient.newBuilder()
      .dispatcher(dispatcher)
      .build()
    new DefaultKubernetesClient(httpClientWithCustomDispatcher, config)
  }

  private implicit class OptionConfigurableConfigBuilder(val configBuilder: ConfigBuilder)
    extends AnyVal {

    def withOption[T]
    (option: Option[T])
    (configurator: ((T, ConfigBuilder) => ConfigBuilder)): ConfigBuilder = {
      option.map { opt =>
        configurator(opt, configBuilder)
      }.getOrElse(configBuilder)
    }
  }
}

