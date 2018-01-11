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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ipc;

import com.tencent.angel.utils.Configurable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/** Base transport class used by ClientCache}. */
public abstract class Transceiver implements Closeable, Configurable {
  private final ReentrantLock channelLock = new ReentrantLock();

  public abstract String getRemoteName() throws IOException;

  /**
   * Acquires an exclusive lock on the transceiver's channel.
   */
  public void lockChannel() {
    channelLock.lock();
  }

  /**
   * Releases the lock on the transceiver's channel if held by the calling thread.
   */
  public void unlockChannel() {
    if (channelLock.isHeldByCurrentThread()) {
      channelLock.unlock();
    }
  }

  /**
   * default calls {@link #writeBuffers(java.util.List)} followed by {@link #readBuffers()}.
   */
  public List<ByteBuffer> transceive(List<ByteBuffer> request) throws IOException {
    lockChannel();
    try {
      writeBuffers(request);
      return readBuffers();
    } finally {
      unlockChannel();
    }
  }

  /**
   * messages using callbacks.
   */
  public void transceive(List<ByteBuffer> request, Callback<List<ByteBuffer>> callback)
      throws IOException {
    // The default implementation works synchronously
    try {
      List<ByteBuffer> response = transceive(request);
      callback.handleResult(response);
    } catch (IOException e) {
      callback.handleError(e);
    }
  }

  /** Called by the default definition of {@link #transceive(java.util.List)}. */
  public abstract List<ByteBuffer> readBuffers() throws IOException;

  public abstract void writeBuffers(List<ByteBuffer> buffers) throws IOException;

  /**
   * True if a handshake has been completed for this connection. Used to determine whether a
   * handshake need be completed prior to a one-way message. Requests and responses are always
   * prefixed by handshakes, but one-way messages. If the first request sent over a connection is
   * one-way, then a handshake-only response is returned. Subsequent one-way messages over the
   * connection will have no response data sent. Returns false by default.
   */
  public boolean isConnected() {
    return false;
  }

  public void close() throws IOException {}
}
