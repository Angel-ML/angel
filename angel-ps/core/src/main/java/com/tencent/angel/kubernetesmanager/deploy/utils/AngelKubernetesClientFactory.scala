/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
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

