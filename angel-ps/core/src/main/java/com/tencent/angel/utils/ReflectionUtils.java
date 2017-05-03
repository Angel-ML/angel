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

package com.tencent.angel.utils;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReflectionUtils {
  private static final Class<?>[] EMPTY_ARRAY = new Class[0];

  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap();

  private static ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

  private static long previousLogTime = 0L;


  public static void setConf(Object theObject, Configuration conf) {
    if (conf != null) {
      if ((theObject instanceof Configurable)) {
        ((Configurable) theObject).setConf(conf);
      }
      setJobConf(theObject, conf);
    }
  }

  private static void setJobConf(Object theObject, Configuration conf) {
    try {
      Class jobConfClass = conf.getClassByName("org.apache.hadoop.mapred.JobConf");

      Class jobConfigurableClass = conf.getClassByName("org.apache.hadoop.mapred.JobConfigurable");

      if ((jobConfClass.isAssignableFrom(conf.getClass()))
          && (jobConfigurableClass.isAssignableFrom(theObject.getClass()))) {
        Method configureMethod =
            jobConfigurableClass.getMethod("configure", new Class[] {jobConfClass});

        configureMethod.invoke(theObject, new Object[] {conf});
      }
    } catch (ClassNotFoundException e) {
    } catch (Exception e) {
      throw new RuntimeException("Error in configuring object", e);
    }
  }

  public static <T> T newInstance(Class<T> theClass, Configuration conf) {
    Object result;
    try {
      Constructor meth = (Constructor) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[0]);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    setConf(result, conf);
    return (T) result;
  }

  public static void setContentionTracing(boolean val) {
    threadBean.setThreadContentionMonitoringEnabled(val);
  }

  private static String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  public static void printThreadInfo(PrintWriter stream, String title) {
    int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: " + title);
    stream.println(threadIds.length + " active threads");
    for (long tid : threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, 20);
      if (info == null) {
        stream.println("  Inactive");
      } else {
        stream.println("Thread " + getTaskName(info.getThreadId(), info.getThreadName()) + ":");

        Thread.State state = info.getThreadState();
        stream.println("  State: " + state);
        stream.println("  Blocked count: " + info.getBlockedCount());
        stream.println("  Waited count: " + info.getWaitedCount());
        if (contention) {
          stream.println("  Blocked time: " + info.getBlockedTime());
          stream.println("  Waited time: " + info.getWaitedTime());
        }
        if (state == Thread.State.WAITING) {
          stream.println("  Waiting on " + info.getLockName());
        } else if (state == Thread.State.BLOCKED) {
          stream.println("  Blocked on " + info.getLockName());
          stream.println("  Blocked by "
              + getTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
        }

        stream.println("  Stack:");
        for (StackTraceElement frame : info.getStackTrace())
          stream.println("    " + frame.toString());
      }
    }
    stream.flush();
  }

  public static void logThreadInfo(Logger log, String title, long minInterval) {
    boolean dumpStack = false;
    if (log.isInfoEnabled()) {
      synchronized (ReflectionUtils.class) {
        long now = System.currentTimeMillis();
        if (now - previousLogTime >= minInterval * 1000L) {
          previousLogTime = now;
          dumpStack = true;
        }
      }
      if (dumpStack) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        printThreadInfo(new PrintWriter(buffer), title);
        log.info(buffer.toString());
      }
    }
  }

  public static <T> Class<T> getClass(T o) {
    return (Class<T>) o.getClass();
  }

  static void clearCache() {
    CONSTRUCTOR_CACHE.clear();
  }

  static int getCacheSize() {
    return CONSTRUCTOR_CACHE.size();
  }

  // private static SerializationFactory getFactory(Configuration conf) {
  // if (serialFactory == null) {
  // serialFactory = new SerializationFactory(conf);
  // }
  // return serialFactory;
  // }

  // public static <T> T copy(Configuration conf, T src, T dst)
  // throws IOException {
  // CopyInCopyOutBuffer buffer = (CopyInCopyOutBuffer) cloneBuffers.get();
  // buffer.outBuffer.reset();
  // SerializationFactory factory = getFactory(conf);
  // Class cls = src.getClass();
  // Serializer serializer = factory.getSerializer(cls);
  // serializer.open(buffer.outBuffer);
  // serializer.serialize(src);
  // buffer.moveData();
  // Deserializer deserializer = factory.getDeserializer(cls);
  // deserializer.open(buffer.inBuffer);
  // dst = deserializer.deserialize(dst);
  // return dst;
  // }
  //
  // @Deprecated
  // public static void cloneWritableInto(Writable dst, Writable src) throws IOException {
  // CopyInCopyOutBuffer buffer = (CopyInCopyOutBuffer) cloneBuffers.get();
  // buffer.outBuffer.reset();
  // src.write(buffer.outBuffer);
  // buffer.moveData();
  // dst.readFields(buffer.inBuffer);
  // }
  //
  // private static class CopyInCopyOutBuffer {
  // DataOutputBuffer outBuffer = new DataOutputBuffer();
  // DataInputBuffer inBuffer = new DataInputBuffer();
  //
  // void moveData() {
  // this.inBuffer.reset(this.outBuffer.getData(), this.outBuffer.getLength());
  // }
  // }
}
