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

package com.tencent.angel.psagent;

import com.tencent.angel.common.AngelCounter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CounterUpdater {
  private static final Log LOG = LogFactory.getLog(CounterUpdater.class);
  private long initCpuCumulativeTime = 0;
  private GcTimeUpdater gcUpdater;
  private ResourceCalculatorProcessTree pTree;
  
  /** A Map where Key-> URIScheme and value->FileSystemStatisticUpdater*/
  private Map<String, FileSystemStatisticUpdater> statisticUpdaters =
      new HashMap<String, FileSystemStatisticUpdater>();

  class GcTimeUpdater {
    private long lastGcMillis = 0;
    private List<GarbageCollectorMXBean> gcBeans = null;

    public GcTimeUpdater() {
      this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
      getElapsedGc();
    }

    /**
     * @return the number of milliseconds that the gc has used for CPU since the last time this
     *         method was called.
     */
    protected long getElapsedGc() {
      long thisGcMillis = 0;
      for (GarbageCollectorMXBean gcBean : gcBeans) {
        thisGcMillis += gcBean.getCollectionTime();
      }

      long delta = thisGcMillis - lastGcMillis;
      this.lastGcMillis = thisGcMillis;
      return delta;
    }

    /**
     * Increment the gc-elapsed-time counter.
     */
    private void incrementGcCounter() {
      if (null == PSAgentContext.get().getMetrics()) {
        return; // nothing to do.
      }
      long elapsedGc = getElapsedGc();
      long totalGc = 0;
      String gc_time = PSAgentContext.get().getMetrics().get(AngelCounter.GC_TIME_MILLIS);
      if (gc_time != null) {
        totalGc = elapsedGc + Long.parseLong(gc_time);
      } else {
        totalGc = elapsedGc;
      }
      PSAgentContext.get().getMetrics().put(AngelCounter.GC_TIME_MILLIS, Long.toString(totalGc));
    }
  }

  class FileSystemStatisticUpdater {
    private List<FileSystem.Statistics> stats;
    private String schema;

    FileSystemStatisticUpdater(List<FileSystem.Statistics> stats, String schema) {
      this.stats = stats;
      this.schema = schema;
    }

    void updateCounters() {
      String counterPrifix = schema.toUpperCase() + "_";
      long readBytes = 0;
      long writeBytes = 0;
      long readOps = 0;
      long largeReadOps = 0;
      long writeOps = 0;
      for (FileSystem.Statistics stat : stats) {
        readBytes = readBytes + stat.getBytesRead();
        writeBytes = writeBytes + stat.getBytesWritten();
        readOps = readOps + stat.getReadOps();
        largeReadOps = largeReadOps + stat.getLargeReadOps();
        writeOps = writeOps + stat.getWriteOps();
      }
      PSAgentContext.get().getMetrics()
          .put(counterPrifix + AngelCounter.BYTES_READ, Long.toString(readBytes));
      PSAgentContext.get().getMetrics()
          .put(counterPrifix.toString() + AngelCounter.BYTES_WRITTEN, Long.toString(writeBytes));
      PSAgentContext.get().getMetrics()
          .put(counterPrifix + AngelCounter.READ_OPS, Long.toString(readOps));
      PSAgentContext.get().getMetrics()
          .put(counterPrifix + AngelCounter.LARGE_READ_OPS, Long.toString(largeReadOps));
      PSAgentContext.get().getMetrics()
          .put(counterPrifix + AngelCounter.WRITE_OPS, Long.toString(writeOps));
    }
  }


  public synchronized void updateCounters() {
    Map<String, List<FileSystem.Statistics>> map =
        new HashMap<String, List<FileSystem.Statistics>>();
    for (Statistics stat : FileSystem.getAllStatistics()) {
      String uriScheme = stat.getScheme();
      if (map.containsKey(uriScheme)) {
        List<FileSystem.Statistics> list = map.get(uriScheme);
        list.add(stat);
      } else {
        List<FileSystem.Statistics> list = new ArrayList<FileSystem.Statistics>();
        list.add(stat);
        map.put(uriScheme, list);
      }
    }
    for (Map.Entry<String, List<FileSystem.Statistics>> entry : map.entrySet()) {
      FileSystemStatisticUpdater updater = statisticUpdaters.get(entry.getKey());
      if (updater == null) {// new FileSystem has been found in the cache
        updater = new FileSystemStatisticUpdater(entry.getValue(), entry.getKey());
        statisticUpdaters.put(entry.getKey(), updater);
      }
      updater.updateCounters();
    }

    gcUpdater.incrementGcCounter();
    updateResourceCounters();
  }

  @SuppressWarnings("deprecation")
  private void updateResourceCounters() {
    // Update generic resource counters
    updateHeapUsageCounter();

    // Updating resources specified in ResourceCalculatorProcessTree
    if (pTree == null) {
      return;
    }
    pTree.updateProcessTree();
    long cpuTime = pTree.getCumulativeCpuTime();
    long pMem = pTree.getCumulativeRssmem();
    long vMem = pTree.getCumulativeVmem();
    // Remove the CPU time consumed previously by JVM reuse
    cpuTime -= initCpuCumulativeTime;
    PSAgentContext.get().getMetrics().put(AngelCounter.CPU_MILLISECONDS, Long.toString(cpuTime));
    PSAgentContext.get().getMetrics().put(AngelCounter.PHYSICAL_MEMORY_BYTES, Long.toString(pMem));
    PSAgentContext.get().getMetrics().put(AngelCounter.VIRTUAL_MEMORY_BYTES, Long.toString(vMem));
  }

  /**
   * Updates the {@link TaskCounter#COMMITTED_HEAP_BYTES} counter to reflect the current total
   * committed heap space usage of this JVM.
   */
  private void updateHeapUsageCounter() {
    long currentHeapUsage = Runtime.getRuntime().totalMemory();
    PSAgentContext.get().getMetrics()
        .put(AngelCounter.COMMITTED_HEAP_BYTES, Long.toString(currentHeapUsage));
  }

  public CounterUpdater() {
    this.gcUpdater = new GcTimeUpdater();
  }

  public void initialize() {
    Class<? extends ResourceCalculatorProcessTree> clazz =
        PSAgentContext
            .get()
            .getConf()
            .getClass(MRConfig.RESOURCE_CALCULATOR_PROCESS_TREE, null,
                ResourceCalculatorProcessTree.class);
    pTree =
        ResourceCalculatorProcessTree.getResourceCalculatorProcessTree(
            System.getenv().get("JVM_PID"), clazz, PSAgentContext.get().getConf());
    if (pTree != null) {
      pTree.updateProcessTree();
      initCpuCumulativeTime = pTree.getCumulativeCpuTime();
    }
    LOG.info(" Using ResourceCalculatorProcessTree : " + pTree);
  }
}
