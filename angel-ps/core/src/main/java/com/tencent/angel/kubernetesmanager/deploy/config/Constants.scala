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
package com.tencent.angel.kubernetesmanager.deploy.config

private[angel] object Constants {

  // Labels
  val ANGEL_APP_ID_LABEL = "angel-app-selector"
  val ANGEL_EXECUTOR_ID_LABEL = "angel-exec-id"
  val ANGEL_ROLE_LABEL = "angel-role"
  val ANGEL_POD_MASTER_ROLE = "master"
  val ANGEL_POD_PS_ROLE = "ps"
  val ANGEL_POD_WORKER_ROLE = "worker"

  // Credentials secrets
  val MASTER_CREDENTIALS_SECRETS_BASE_DIR =
    "/mnt/secrets/angel-kubernetes-credentials"
  val MASTER_CREDENTIALS_CA_CERT_SECRET_NAME = "ca-cert"
  val MASTER_CREDENTIALS_CA_CERT_PATH =
    s"$MASTER_CREDENTIALS_SECRETS_BASE_DIR/$MASTER_CREDENTIALS_CA_CERT_SECRET_NAME"
  val MASTER_CREDENTIALS_CLIENT_KEY_SECRET_NAME = "client-key"
  val MASTER_CREDENTIALS_CLIENT_KEY_PATH =
    s"$MASTER_CREDENTIALS_SECRETS_BASE_DIR/$MASTER_CREDENTIALS_CLIENT_KEY_SECRET_NAME"
  val MASTER_CREDENTIALS_CLIENT_CERT_SECRET_NAME = "client-cert"
  val MASTER_CREDENTIALS_CLIENT_CERT_PATH =
    s"$MASTER_CREDENTIALS_SECRETS_BASE_DIR/$MASTER_CREDENTIALS_CLIENT_CERT_SECRET_NAME"
  val MASTER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME = "oauth-token"
  val MASTER_CREDENTIALS_OAUTH_TOKEN_PATH =
    s"$MASTER_CREDENTIALS_SECRETS_BASE_DIR/$MASTER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME"
  val MASTER_CREDENTIALS_SECRET_VOLUME_NAME = "kubernetes-credentials"

  val MASTER_PORT_NAME = "master-rpc-port"

  // Environment Variables
  val ENV_MASTER_MEMORY = "ANGEL_MASTER_MEMORY"
  val ENV_MASTER_POD_NAME = "ANGEL_MASTER_POD_NAME"
  val ENV_EXECUTOR_POD_NAME_PREFIX = "ANGEL_EXECUTOR_POD_NAME_PREFIX"
  val ENV_EXECUTOR_CORES = "ANGEL_EXECUTOR_CORES"
  val ENV_EXECUTOR_MEMORY = "ANGEL_EXECUTOR_MEMORY"
  val ENV_APPLICATION_ID = "ANGEL_APPLICATION_ID"
  val ENV_EXECUTOR_ID = "ANGEL_EXECUTOR_ID"
  val ENV_EXECUTOR_ATTEMPT_ID = "ANGEL_EXECUTOR_ATTEMPT_ID"
  val ENV_EXECUTOR_POD_IP = "ANGEL_EXECUTOR_POD_IP"
  val ENV_JAVA_OPT_PREFIX = "ANGEL_JAVA_OPT_"
  val ENV_CLASSPATH = "ANGEL_CLASSPATH"
  val ENV_MASTER_BIND_ADDRESS = "ANGEL_MASTER_BIND_ADDRESS"
  val ENV_MASTER_BIND_PORT = "ANGEL_MASTER_BIND_PORT"
  val ENV_ANGEL_USER_TASK = "ANGEL_USER_TASK"
  val ENV_ANGEL_WORKERGROUP_NUMBER = "ANGEL_WORKERGROUP_NUMBER"
  val ENV_ANGEL_TASK_NUMBER = "ANGEL_TASK_NUMBER"
  val ENV_ANGEL_CONF_DIR = "ANGEL_CONF_DIR"
  // Angel app configs for containers
  val ANGEL_CONF_VOLUME = "angel-conf-volume"
  val ANGEL_CONF_DIR_INTERNAL = "/opt/resource/conf"
  val ANGEL_CONF_FILE_NAME = "angel-site.properties"
  val ANGEL_CONF_PATH = s"$ANGEL_CONF_DIR_INTERNAL/$ANGEL_CONF_FILE_NAME"

  val MASTER_CONTAINER_NAME = "angel-kubernetes-master"

  val ANGEL_EXECUTOR_ID = "angel.kubernetes.executor.id"
  val ANGEL_EXECUTOR_ATTEMPT_ID = "angel.kubernetes.executor.attempt.id"
}
