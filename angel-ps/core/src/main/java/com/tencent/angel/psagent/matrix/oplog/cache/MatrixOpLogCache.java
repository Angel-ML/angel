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

package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.math.TUpdate;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixOpLogType;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.MatrixClientAdapter;
import com.tencent.angel.psagent.task.TaskContext;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.tencent.angel.ml.matrix.RowType;

/**
 * Matrix oplog(updates) cache.
 */
public class MatrixOpLogCache {
  private static final Log LOG = LogFactory.getLog(MatrixOpLogCache.class);

  /** matrix id to cache map */
  private final ConcurrentHashMap<Integer, MatrixOpLog> opLogs;

  /** thread pool for workers that merge updates and flush updates to ps */
  private ExecutorService workerPool;

  /** oplog message(request) queue, there are three type request now:flush, clock and merge */
  private final PriorityBlockingQueue<OpLogMessage> messageQueue;

  /** matrix id -> (message sequence id -> message) */
  private final Int2ObjectOpenHashMap<Int2ObjectAVLTreeMap<OpLogMessage>> seqIdToMessageMaps;

  /** matrix id to waiting merge requests list map */
  private final Int2ObjectOpenHashMap<LinkedBlockingQueue<OpLogMessage>> waitedMessageQueues;

  /** stop message dispatcher and workers */
  private final AtomicBoolean stopped;

  /** matrix id to merging update counter map */
  private final Int2IntOpenHashMap mergingCounters;

  /** blocked flush/clock request */
  private final Int2ObjectOpenHashMap<List<OpLogMessage>> flushListeners;

  /** message sequence id generator */
  private final AtomicInteger seqIdGenerator;

  /** request to result map */
  private final ConcurrentHashMap<OpLogMessage, Future<VoidResult>> messageToFutureMap;

  /** request dispatcher */
  private Thread dispatcher;

  public MatrixOpLogCache() {
    opLogs = new ConcurrentHashMap<>();

    messageQueue = new PriorityBlockingQueue<OpLogMessage>(100, new PriorityComparator());
    seqIdToMessageMaps = new Int2ObjectOpenHashMap<Int2ObjectAVLTreeMap<OpLogMessage>>();
    waitedMessageQueues = new Int2ObjectOpenHashMap<LinkedBlockingQueue<OpLogMessage>>();
    flushListeners = new Int2ObjectOpenHashMap<List<OpLogMessage>>();
    seqIdGenerator = new AtomicInteger(0);
    mergingCounters = new Int2IntOpenHashMap();
    stopped = new AtomicBoolean(false);
    messageToFutureMap = new ConcurrentHashMap<OpLogMessage, Future<VoidResult>>();
  }

  /**
   * Start matrix update merge/flush tasks dispatcher and initialize workers thread pool for
   * merge/flush tasks
   */
  public void start() {
    workerPool = Executors.newFixedThreadPool(PSAgentContext.get().getConf().getInt(
      AngelConf.ANGEL_MATRIX_OPLOG_MERGER_POOL_SIZE,
      AngelConf.DEFAULT_ANGEL_MATRIX_OPLOG_MERGER_POOL_SIZE));

    dispatcher = new MergeDispacher();
    dispatcher.setName("oplog-merge-dispatcher");
    dispatcher.start();
  }

  /**
   * Stop all merge/flush tasks and dispatcher
   */
  public void stop() {
    if (!stopped.getAndSet(true)) {
      if(workerPool != null) {
        workerPool.shutdownNow();
        workerPool = null;
      }

      if (dispatcher != null) {
        dispatcher.interrupt();
        try {
          dispatcher.join();
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException while stopping");
        }
        dispatcher = null;
      }
      return;
    }
  }

  class PriorityComparator implements Comparator<OpLogMessage> {
    @Override
    public int compare(OpLogMessage o1, OpLogMessage o2) {
      return o1.getSeqId() - o2.getSeqId();
    }
  }

  /**
   * Flush the updates in cache to parameter servers
   * 
   * @param context task context
   * @param matrixId matrix id
   * @return Future<VoidResult> a future result that caller can wait for result by get()
   */
  public Future<VoidResult> flush(TaskContext context, int matrixId) {
    FutureResult<VoidResult> futureResult = new FutureResult<VoidResult>();
    try {
      // Generate a flush request and put it to request queue
      OpLogMessage flushMessage =
          new OpLogMessage(seqIdGenerator.incrementAndGet(), matrixId, OpLogMessageType.FLUSH,
              context);
      messageToFutureMap.put(flushMessage, futureResult);
      messageQueue.add(flushMessage);
    } catch (IllegalStateException e) {
      LOG.warn("oplog clock failed, ", e);
      futureResult.set(new VoidResult(ResponseType.FAILED));
    }
    return futureResult;
  }

  /**
   * Update matrix partition clocks
   * 
   * @param context task context
   * @param matrixId matrix id matrix id
   * @param flushFirst true means we need flush the matrix updates first
   * @return Future<VoidResult> a future result that caller can wait for result by get()
   */
  public Future<VoidResult> clock(TaskContext context, int matrixId, boolean flushFirst) {
    FutureResult<VoidResult> futureResult = new FutureResult<VoidResult>();
    try {
      // Generate a clock request and put it to request queue
      LOG.debug("task " + context.getIndex() + " clock matrix " + matrixId);
      ClockMessage clockMessage =
          new ClockMessage(seqIdGenerator.incrementAndGet(), matrixId, context, flushFirst);
      messageToFutureMap.put(clockMessage, futureResult);
      messageQueue.add(clockMessage);
    } catch (IllegalStateException e) {
      LOG.warn("oplog clock failed, ", e);
      futureResult.set(new VoidResult(ResponseType.FAILED));
    }
    return futureResult;
  }

  /**
   * Put a matrix update to the cache
   * 
   * @param context task context
   * @param delta matrix update
   */
  public void increment(TaskContext context, TUpdate delta) {
    try {
      // Generate a merge request and put it to request queue
      messageQueue.add(new OpLogMergeMessage(seqIdGenerator.incrementAndGet(), context, delta));
    } catch (IllegalStateException e) {
      LOG.warn("oplog flush failed, ", e);
    }
  }

  /**
   * Message dispatcher
   */
  class MergeDispacher extends Thread {
    @Override
    public void run() {
      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        OpLogMessage message;
        try {
          message = messageQueue.take();
        } catch (InterruptedException e) {
          LOG.warn("oplog-merge-dispatcher interrupted");
          return;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("receive oplog request " + message);
        }

        int matrixId = message.getMatrixId();
        switch (message.getType()) {
          case MERGE: {
            // If the matrix op log cache does not exist for the matrix, create a new one for the
            // matrix
            // and add it to cache maps
            if (!opLogs.containsKey(matrixId)) {
              mergingCounters.put(matrixId, 0);
              addMatrixOpLog(matrixId);
            }

            if (!seqIdToMessageMaps.containsKey(matrixId)) {
              seqIdToMessageMaps.put(matrixId, new Int2ObjectAVLTreeMap<OpLogMessage>());
            }

            // Add the message to the tree map
            if (!seqIdToMessageMaps.get(matrixId).containsKey(message.getSeqId())) {
              seqIdToMessageMaps.get(matrixId).put(message.getSeqId(), message);
            }

            if (needWait(message.getMatrixId(), message.getSeqId())) {
              // If there are flush / clock requests blocked, we need to put this merge request into
              // the waiting queue
              LOG.debug("add message=" + message + " to wait queue");
              addWaitQueue(message);
            } else {
              // Launch a merge worker to merge the update to matrix op log cache
              mergingCounters.addTo(message.getMatrixId(), 1);
              merge((OpLogMergeMessage) message);
            }
            break;
          }

          case MERGE_SUCCESS: {
            if (LOG.isDebugEnabled()) {
              LOG.debug(printMergingCounters());
            }
            // Remove the message from the tree map
            seqIdToMessageMaps.get(matrixId).remove(message.getSeqId());
            mergingCounters.addTo(message.getMatrixId(), -1);

            // Wake up blocked flush/clock request
            checkAndWakeUpListeners(message.getMatrixId());
            break;
          }

          case CLOCK:
          case FLUSH: {
            // Add flush/clock request to listener list to waiting for all the existing
            // updates are merged
            addToListenerList(message);
            // Wake up blocked flush/clock request
            checkAndWakeUpListeners(message.getMatrixId());
            break;
          }
        }
      }
    }

    private void checkAndWakeUpListeners(int matrixId) {
      // If all updates are merged for this matrix, we need wake up flush/clock requests which are
      // blocked.
      if (mergingCounters.get(matrixId) == 0) {

        // Get next merge message sequence id
        int endPos = 0;
        if (seqIdToMessageMaps.get(matrixId) == null || seqIdToMessageMaps.get(matrixId).isEmpty()) {
          endPos = Integer.MAX_VALUE;
        } else {
          endPos = seqIdToMessageMaps.get(matrixId).firstIntKey();
        }

        wakeup(matrixId, endPos);
      }
    }

    private void addToListenerList(OpLogMessage message) {
      List<OpLogMessage> listeners = flushListeners.get(message.getMatrixId());
      if (listeners == null) {
        listeners = new ArrayList<OpLogMessage>();
        flushListeners.put(message.getMatrixId(), listeners);
      }
      listeners.add(message);
    }

    private void addWaitQueue(OpLogMessage message) {
      LinkedBlockingQueue<OpLogMessage> queue = waitedMessageQueues.get(message.getMatrixId());
      if (queue == null) {
        queue = new LinkedBlockingQueue<OpLogMessage>();
        waitedMessageQueues.put(message.getMatrixId(), queue);
      }
      queue.add(message);
    }

    private boolean needWait(int matrixId, int seqId) {
      List<OpLogMessage> listeners = flushListeners.get(matrixId);
      if (listeners != null) {
        int size = listeners.size();
        for (int i = 0; i < size; i++) {
          if (listeners.get(i).getSeqId() < seqId) {
            return true;
          }
        }
      }

      return false;
    }

    private void wakeup(int matrixId, int currentMergePos) {
      // Wake up listeners(flush/clock requests) that have little sequence id than current merge
      // position
      wakeupListeners(matrixId, currentMergePos);
      // Wake up blocked merge requests
      wakeupWaitMessages(matrixId);
    }

    private void wakeupListeners(int matrixId, int currentMergePos) {
      LOG.debug("wakeup clock listeners for matrix " + matrixId);
      List<OpLogMessage> messages = flushListeners.get(matrixId);
      if (messages != null) {
        Iterator<OpLogMessage> iter = messages.iterator();
        while (iter.hasNext()) {
          OpLogMessage message = iter.next();
          LOG.debug("currentMergePos=" + currentMergePos + ", message.getSeqId()="
              + message.getSeqId());
          if (currentMergePos > message.getSeqId()) {
            LOG.debug("clock opLogs for " + matrixId + ", oplog=" + opLogs.get(matrixId));
            if (message.getType() == OpLogMessageType.CLOCK) {
              ClockMessage clockMessage = (ClockMessage) message;
              if (!clockMessage.isFlushFirst()) {
                clock(clockMessage, null);
              } else {
                clock(clockMessage, opLogs.remove(matrixId));
              }
            } else {
              flush(message, opLogs.remove(matrixId));
            }

            iter.remove();
          } else {
            break;
          }
        }
      }

    }

    private void wakeupWaitMessages(int matrixId) {
      int nextEndPos = getWakeEndPos(matrixId);
      LOG.debug("next wakeup endPos=" + nextEndPos);
      LinkedBlockingQueue<OpLogMessage> waitQueue = waitedMessageQueues.get(matrixId);
      if (waitQueue != null) {
        OpLogMessage message = null;
        while (((message = waitQueue.peek()) != null) && (message.getSeqId() < nextEndPos)) {
          waitQueue.poll();
          messageQueue.add(message);
        }
      }
    }

    private int getWakeEndPos(int matrixId) {
      return getWakeEndPosFromListeners(matrixId);
    }

    private int getWakeEndPosFromListeners(int matrixId) {
      int endPos = Integer.MAX_VALUE;
      // Get minimal sequence id from listeners
      List<OpLogMessage> messages = flushListeners.get(matrixId);
      if (messages != null) {
        int size = messages.size();
        for (int i = 0; i < size; i++) {
          LOG.debug("clock message=" + messages.get(i));
          if (messages.get(i).getSeqId() < endPos) {
            endPos = messages.get(i).getSeqId();
          }
        }
      }

      return endPos;
    }

    private String printMergingCounters() {
      StringBuilder sb = new StringBuilder();
      sb.append("merging counter for each matrix \n");
      sb.append("matrixId counter \n");
      for (it.unimi.dsi.fastutil.ints.Int2IntMap.Entry entry : mergingCounters.int2IntEntrySet()) {
        sb.append(entry.getIntKey());
        sb.append("\t");
        sb.append(entry.getIntValue());
        sb.append("\n");
      }

      return sb.toString();
    }

    private void merge(OpLogMergeMessage message) {
      workerPool.execute(new Merger(message));
    }

    private void flush(OpLogMessage message, MatrixOpLog opLog) {
      workerPool.execute(new Flusher(message, opLog));
    }

    private void clock(OpLogMessage message, MatrixOpLog opLog) {
      workerPool.execute(new Flusher(message, opLog));
    }
  }

  /**
   * Thread that merge the updates
   */
  class Merger extends Thread {
    private final OpLogMergeMessage message;

    public Merger(OpLogMergeMessage message) {
      this.message = message;
    }

    @Override
    public void run() {
      try {
        opLogs.get(message.getMatrixId()).merge(message.getUpdate());
        merged(message);
      } catch (InterruptedException e) {
        LOG.warn("merge " + message + " is interruped");
      } catch (Throwable e) {
        LOG.fatal("merge " + message + " falied, ", e);
        PSAgentContext.get().getPsAgent().error("merge " + message + " falied, " + e.getMessage());
      }
    }
  }

  /**
   * Thread that flush the updates
   */
  class Flusher extends Thread {
    private final OpLogMessage message;
    private final MatrixOpLog matrixOpLog;

    public Flusher(OpLogMessage message, MatrixOpLog matrixOpLog) {
      this.message = message;
      this.matrixOpLog = matrixOpLog;
    }

    @Override
    public void run() {
      MatrixClientAdapter matrixClientAdapter = PSAgentContext.get().getMatrixClientAdapter();
      try {
        if ((matrixOpLog != null) && needFlushLocalStorage(message.getMatrixId())) {
          matrixOpLog.flushToLocalStorage();
        }

        Future<VoidResult> flushFuture =
            matrixClientAdapter.flush(message.getMatrixId(), message.getContext(), matrixOpLog,
                message.getType() == OpLogMessageType.CLOCK);
        VoidResult result = flushFuture.get();
        ((FutureResult<VoidResult>) messageToFutureMap.remove(message)).set(result);
      } catch (Throwable e) {
        LOG.fatal("flush op " + message + " failed, ", e);
        PSAgentContext.get().getPsAgent().error("flush op " + message + " falied, " + e.getMessage());
      }
    }

    private boolean needFlushLocalStorage(int matrixId) {
      int stalness = PSAgentContext.get().getStaleness();
      MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
      int localTaskNum = PSAgentContext.get().getLocalTaskNum();

      // If hogwild mode is enabled on the number of local task is more than 1 on SSP mode, we
      // should flush updates to local matrix storage
      return stalness > 0 && !matrixMeta.isHogwild() && localTaskNum > 1;
    }

    public OpLogMessage getMessage() {
      return message;
    }
  }

  private void addMatrixOpLog(int matrixId) {
    opLogs.put(matrixId, createMatrixOpLog(PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId)));
  }

  private MatrixOpLog createMatrixOpLog(MatrixMeta matrixMeta) {
    int matrixId = matrixMeta.getId();
    String type =
      matrixMeta.getAttribute(MatrixConf.MATRIX_OPLOG_TYPE);
    boolean enableFilter =
      matrixMeta.getAttribute(MatrixConf.MATRIX_OPLOG_ENABLEFILTER,
        MatrixConf.DEFAULT_MATRIX_OPLOG_ENABLEFILTER).equalsIgnoreCase("true");

    if(type == null) {
      RowType rowType = matrixMeta.getRowType();
      switch(rowType) {
        case T_DOUBLE_DENSE:
          return new DenseDoubleMatrixOpLog(matrixId, enableFilter);
        case T_DOUBLE_SPARSE:
          return new SparseDoubleMatrixOplog(matrixId, enableFilter);
        case T_INT_DENSE:
          return new DenseIntMatrixOpLog(matrixId, enableFilter);
        case T_INT_SPARSE:
          return new SparseIntMatrixOpLog(matrixId, enableFilter);
        case T_FLOAT_DENSE:
          return new DenseFloatMatrixOplog(matrixId, enableFilter);
        case T_FLOAT_SPARSE:
          return new SparseFloatMatrixOpLog(matrixId, enableFilter);
        case T_DOUBLE_SPARSE_LONGKEY:
          return new SparseDoubleLongKeyMatrixOpLog(matrixId, enableFilter);
        case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
          return new CompSparseDoubleLongKeyMatrixOpLog(matrixId, enableFilter);
        case T_DOUBLE_SPARSE_COMPONENT:
          return new CompSparseDoubleMatrixOpLog(matrixId, enableFilter);
        case T_FLOAT_SPARSE_COMPONENT:
          return new CompSparseFloatMatrixOpLog(matrixId, enableFilter);
        case T_INT_SPARSE_COMPONENT:
          return new CompSparseIntMatrixOpLog(matrixId, enableFilter);
      }
    } else {
      MatrixOpLogType opLogType = MatrixOpLogType.valueOf(type);
      switch (opLogType) {
        case DENSE_DOUBLE:
          return new DenseDoubleMatrixOpLog(matrixId, enableFilter);
        case SPARSE_DOUBLE:
          return new SparseDoubleMatrixOplog(matrixId, enableFilter);
        case DENSE_INT:
          return new DenseIntMatrixOpLog(matrixId, enableFilter);
        case SPARSE_INT:
          return new SparseIntMatrixOpLog(matrixId, enableFilter);
        case DENSE_FLOAT:
          return new DenseFloatMatrixOplog(matrixId, enableFilter);
        case SPARSE_FLOAT:
          return new SparseFloatMatrixOpLog(matrixId, enableFilter);
        case SPARSE_DOUBLE_LONGKEY:
          return new SparseDoubleLongKeyMatrixOpLog(matrixId, enableFilter);
        case COMPONENT_SPARSE_DOUBLE:
          return new CompSparseDoubleMatrixOpLog(matrixId, enableFilter);
        case COMPONENT_SPARSE_FLOAT:
          return new CompSparseFloatMatrixOpLog(matrixId, enableFilter);
        case COMPONENT_SPARSE_INT:
          return new CompSparseIntMatrixOpLog(matrixId, enableFilter);
        case COMPONENT_SPARSE_DOUBLE_LONGKEY:
          return new CompSparseDoubleLongKeyMatrixOpLog(matrixId, enableFilter);
      }
    }

    return new DenseDoubleMatrixOpLog(matrixId, enableFilter);
  }

  private void merged(OpLogMergeMessage message) throws InterruptedException {
    messageQueue.put(new OpLogMessage(message.getSeqId(), message.getMatrixId(),
        OpLogMessageType.MERGE_SUCCESS, message.getContext()));
  }

  public void remove(int matrixId) {
    opLogs.remove(matrixId);
  }
}
