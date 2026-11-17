package com.tencent.angel.kubernetesmanager.deploy.utils

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.tencent.angel.conf.AngelConf
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
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
    val config = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(normalizeMasterUrl(master))
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
    new DefaultKubernetesClient(config)
  }

  private[angel] def normalizeMasterUrl(master: String): String = {
    val uri = new java.net.URI(master)
    require(uri.getScheme == "https" || uri.getScheme == "http", "Kubernetes API URL must use HTTP or HTTPS")
    require(uri.getHost != null, "Kubernetes API URL must specify a host")
    if (uri.getPort >= 0) master
    else new java.net.URI(uri.getScheme, uri.getUserInfo, uri.getHost,
      if (uri.getScheme == "https") 443 else 80, uri.getPath, uri.getQuery, uri.getFragment).toString
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

