/*
 * Tencent is pleased to support the open source community by making Angel available.
 * 
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 * 
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * 
 * https://opensource.org/licenses/BSD-3-Clause
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.tencent.angel.conf;

/**
 * TConstants holds a bunch of ml-related constants
 */
public final class TConstants {
  public static final String ML_CONNECTION_TIMEOUT_MILLIS = "ml.connection.timeout";

  /** If not specified, the default connection timeout will be used (3 sec). */
  public static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 3 * 1000L;

  public static final String CONNECTION_WRITE_TIMEOUT_SEC = "ml.connection.write.timeout";

  /**
   * If not specified, the default connection write timeout will be used (6 sec).
   */
  public static final int DEFAULT_CONNECTION_WRITE_TIMEOUT_SEC = 30;

  public static final String CONNECTION_READ_TIMEOUT_SEC = "ml.connection.read.timeout";

  /**
   * If not specified, the default connection read timeout will be used (300 sec).
   */
  public static final int DEFAULT_CONNECTION_READ_TIMEOUT_SEC = 60 * 5;

  public static final int DEFAULT_READ_TIMEOUT_SEC = 10;

  public static final String SERVER_IO_THREAD = "netty.server.io.threads";
  public static final String NETWORK_IO_MODE = "netty.io.mode";

  public static final String CLIENT_IO_THREAD = "netty.client.io.threads";

  public static int RETRY_BACKOFF[] = {1, 1, 1, 2, 2, 4, 4, 8, 16, 32};

  public static final String ML_CLIENT_OPERATION_TIMEOUT = "ml.client.operation.timeout";

  public static final int DEFAULT_ML_CLIENT_OPERATION_TIMEOUT = Integer.MAX_VALUE;

  public static final String ML_CLIENT_PAUSE = "ml.client.pause";

  public static final long DEFAULT_ML_CLIENT_PAUSE = 1000;

  public static final String ML_CLIENT_RETRIES_NUMBER = "ml.client.retries.number";

  public static final int DEFAULT_ML_CLIENT_RETRIES_NUMBER = 10;

  /**
   * timeout for each RPC
   */
  public static final String ML_RPC_TIMEOUT_KEY = "ml.rpc.timeout";

  public static final int DEFAULT_ML_RPC_TIMEOUT = 60000;

  public static final String ML_CLIENT_RPC_MAXATTEMPTS = "ml.client.rpc.maxattempts";

  /**
   * Default value of {@link #ML_CLIENT_RPC_MAXATTEMPTS}.
   */
  public static final int DEFAULT_ML_CLIENT_RPC_MAXATTEMPTS = 1;
}
