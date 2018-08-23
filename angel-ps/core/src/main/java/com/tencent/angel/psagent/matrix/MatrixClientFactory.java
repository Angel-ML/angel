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


package com.tencent.angel.psagent.matrix;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.task.TaskContext;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Matrix client factory.
 */
public class MatrixClientFactory {
  /**
   * matrix client type class name, default is MatrixClientImpl
   */
  private static String type = MatrixClientImpl.class.getName();

  /**
   * key to matrix client map
   */
  private static ConcurrentHashMap<Key, MatrixClient> cacheClients =
    new ConcurrentHashMap<Key, MatrixClient>();

  /**
   * Set the matrix client type.
   *
   * @param type matrix client type class name
   */
  public static void setType(String type) {
    MatrixClientFactory.type = type;
  }

  /**
   * Get a matrix client.
   *
   * @param matrixName matrix name
   * @param taskIndex  task index
   * @return MatrixClient matrix client
   * @throws InvalidParameterException matrix does not exist
   */
  public static MatrixClient get(String matrixName, int taskIndex)
    throws InvalidParameterException {
    int matrixId = PSAgentContext.get().getMatrixMetaManager().getMatrixId(matrixName);
    if (matrixId == -1) {
      throw new InvalidParameterException("matrix " + matrixName + " does not exist");
    }

    return get(matrixId, taskIndex);
  }

  /**
   * Get a matrix client.
   *
   * @param matrixId matrix id
   * @param taskId   task id
   * @return MatrixClient matrix client
   * @throws InvalidParameterException matrix does not exist
   */
  public static MatrixClient get(int matrixId, int taskId) throws InvalidParameterException {
    if (!PSAgentContext.get().getMatrixMetaManager().exist(matrixId)) {
      throw new InvalidParameterException("matrix with id " + matrixId + " does not exist.");
    }

    Key key = new Key(matrixId, taskId);
    MatrixClient client = cacheClients.get(key);
    if (client == null) {
      try {
        cacheClients.putIfAbsent(key, buildClient(matrixId, taskId, type));
      } catch (Exception x) {
        throw new InvalidParameterException(
          "Invalid matrix client type:" + type.getClass().getName() + ", " + x.getMessage());
      }
      client = cacheClients.get(key);
    }

    return client;
  }

  private static MatrixClient buildClient(int matrix, int taskIndex, String type)
    throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    MatrixClient client = (MatrixClient) (Class.forName(type).newInstance());
    client.setMatrixId(matrix);
    client.setTaskContext(getTaskContext(taskIndex));
    return client;
  }

  private static TaskContext getTaskContext(int taskIndex) {
    return PSAgentContext.get().getTaskContext(taskIndex);
  }

  static class Key {
    private final int matrixId;
    private final int taskId;

    public Key(int matrixId, int taskId) {
      this.matrixId = matrixId;
      this.taskId = taskId;
    }

    public int getMatrixId() {
      return matrixId;
    }

    public int getTaskId() {
      return taskId;
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + matrixId;
      result = prime * result + taskId;
      return result;
    }

    @Override public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Key other = (Key) obj;
      if (matrixId != other.matrixId)
        return false;
      return taskId == other.taskId;
    }

    @Override public String toString() {
      return "Key [matrixId=" + matrixId + ", taskId=" + taskId + "]";
    }
  }

  public static void clear() {
    cacheClients.clear();
  }
}
