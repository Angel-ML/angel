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


package com.tencent.angel.ipc;

import java.io.IOException;

/**
 * Superclass of all protocols that use Hadoop RPC. Subclasses of this interface are also supposed
 * to have a static final long versionID field.
 */
public interface VersionedProtocol {

  /**
   * Return protocol version corresponding to protocol interface.
   *
   * @param protocol      The classname of the protocol interface
   * @param clientVersion The version of the protocol that the client speaks
   * @return the version that the server will speak
   * @throws java.io.IOException if any IO error occurs
   */
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException;
}
