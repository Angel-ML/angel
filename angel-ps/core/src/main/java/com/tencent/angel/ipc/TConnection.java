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

package com.tencent.angel.ipc;

import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ps.impl.PSProtocol;
import com.tencent.angel.worker.WorkerProtocol;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;

/**
 * {@link com.tencent.angel.ipc.TConnectionManager} manages instances of this class.
 * 
 * <p>
 * TConnection instances can be shared. Sharing is usually what you want because rather than each
 * TConnection instance having to do its own cache of partition locations. Sharing makes cleanup of
 * TConnections awkward. See {@link TConnectionManager} for cleanup discussion.
 * 
 * @see com.tencent.angel.ipc.TConnectionManager
 */
public interface TConnection extends Closeable {

  /**
   * @return Configuration instance being used by this TConnection instance.
   */
  public Configuration getConfiguration();

  /**
   * @return true if this connection is closed
   */
  public boolean isClosed();

  /**
   * Get ps rpc client with sync mode.
   * @param hostname ps host name
   * @param port ps listening port
   * @return ps rpc client
   * @throws IOException
   */
  public PSProtocol getPSService(String hostname, int port) throws IOException;

  /**
   * Get ps rpc client with async mode.
   * @param hostname ps host name
   * @param port ps listening port
   * @return ps rpc client
   * @throws IOException
   */
  public PSProtocol.AsyncProtocol getAsyncPSService(String hostname, int port) throws IOException;

  /**
   * Get worker rpc client with sync mode.
   * @param hostname worker host name
   * @param port worker listening port
   * @return worker rpc client
   * @throws IOException
   */
  public WorkerProtocol getWorkerService(String hostname, int port) throws IOException;

  /**
   * Get master rpc client with sync mode.
   * @param hostname master host name
   * @param port master listening port
   * @return master rpc client
   * @throws IOException
   */
  public MasterProtocol getMasterService(String hostname, int port) throws IOException;
}
