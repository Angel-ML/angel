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

package com.tencent.angel.master.task;

import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Task manager in master, it manages all tasks {@link com.tencent.angel.master.task.AMTask} in the application.
 */
public class AMTaskManager {
  private static final Log LOG = LogFactory.getLog(AMTaskManager.class);
  /**task id to task map*/
  private final HashMap<TaskId, AMTask> idToTaskMap;
  private final Lock readLock;
  private final Lock writeLock;
  
  public AMTaskManager(){
    idToTaskMap = new HashMap<TaskId, AMTask>();
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
  }
  
  /**
   * add the task to task map
   * @param taskId the task id
   * @param task the task
   */
  public void putTask(TaskId taskId, AMTask task){
    try{
      writeLock.lock();
      idToTaskMap.put(taskId, task);
    } finally {
      writeLock.unlock();
    }   
  }
  
  /**
   * get the task corresponding to specified task id
   * @param taskId task id
   * @return AMTask the task corresponding to specified task id
   */
  public AMTask getTask(TaskId taskId){
    try{
      readLock.lock();
      return idToTaskMap.get(taskId);
    } finally {
      readLock.unlock();
    }   
  }
  
  /**
   * get all tasks
   * @return Collection<AMTask> all tasks
   */
  public Collection<AMTask> getTasks(){
    try{
      readLock.lock();
      Set<AMTask> cloneTasks = new HashSet<AMTask>();
      cloneTasks.addAll(idToTaskMap.values());
      return cloneTasks;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * write all tasks state to a output stream
   * @param output the output stream
   * @throws IOException
   */
  public void serialize(DataOutputStream output) throws IOException {
    try {
      readLock.lock();
      output.writeInt(idToTaskMap.size());
      for(Entry<TaskId, AMTask> entry:idToTaskMap.entrySet()) {
        output.writeInt(entry.getKey().getIndex());
        entry.getValue().serialize(output);
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * read all tasks state from a input stream
   * @param input the input stream
   * @throws IOException
   */
  public void deserialize(DataInputStream input) throws IOException {
    try {
      writeLock.lock();
      int size = input.readInt();
      for(int i = 0; i < size; i++){
        TaskId taskId = new TaskId(input.readInt());
        AMTask task = new AMTask(taskId, null);
        task.deserialize(input);
        LOG.info("task deserialize=" + task);
        idToTaskMap.put(taskId, task);
      }
    } finally {
      writeLock.unlock();
    }
  }
}
