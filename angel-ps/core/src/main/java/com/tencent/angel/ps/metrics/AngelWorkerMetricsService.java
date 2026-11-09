package com.tencent.angel.ps.metrics;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.metrics.AngelMetrics;
import com.tencent.angel.metrics.AngelMetricsReporter;
import com.tencent.angel.metrics.AngelWorkerLoad;
import com.tencent.angel.metrics.AngelWorkerMetrics;
import com.tencent.angel.metrics.MetricsUtils;
import com.tencent.angel.ps.server.data.profiling.RPCTimes;
import com.tencent.angel.ps.server.data.request.GetUDFRequest;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.request.UpdateUDFRequest;
import com.tencent.angel.psagent.matrix.transport.FailedMsg;
import com.tencent.angel.psagent.matrix.transport.PSFTimes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class AngelWorkerMetricsService {
  
  private final static Log LOG = LogFactory.getLog(AngelWorkerMetricsService.class);

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private final Configuration conf;

  /**
   * Is collect metric
   */
  private final boolean metricEnable;

  private final String sep;

  private final Long reportIntervalMs;

  private final String tid;

  private final String appId;

  /**
   * Report worker metric message to NGCP
   */
  private final MetricsReporter metricsReporter;

  /**
   * metrics about pfs
   */
  private volatile ConcurrentHashMap<String, AngelWorkerMetrics> metricsMap = new ConcurrentHashMap<>();

  /**
   * fail key to fail message map
   */
  private volatile ConcurrentHashMap<FailedMsg, AtomicInteger> keyToFailMsgMap = new ConcurrentHashMap<>();

  public AtomicInteger incrementAtomicInteger(FailedMsg key) {
    if (keyToFailMsgMap.get(key) == null) {
      keyToFailMsgMap.putIfAbsent(key, new AtomicInteger(0));
    }
    keyToFailMsgMap.get(key).incrementAndGet();
    return keyToFailMsgMap.get(key);
  }
  
  public AngelWorkerMetricsService(Configuration conf) {
    this.conf = conf;
    this.metricEnable = conf.getBoolean(AngelConf.ANGEL_METRICS_ENABLE,
        AngelConf.DEFAULT_ANGEL_METRICS_ENABLE);
    this.sep = conf.get(AngelConf.ANGEL_METRICS_DELIMITER,
        AngelConf.DEFAULT_ANGEL_METRICS_DELIMITER);
    this.reportIntervalMs = conf.getLong(
        AngelConf.ANGEL_METRICS_INTERVAL_PERIOD_MILLISECOND,
        AngelConf.DEFAULT_ANGEL_METRICS_INTERVAL_PERIOD_MILLISECOND);
    this.tid = conf.get(AngelConf.ANGEL_METRICS_TID_ANGEL_WORKER,
        AngelConf.DEFAULT_ANGEL_METRICS_TID_ANGEL_WORKER);
    this.appId = conf.get(AngelConf.ANGEL_APPLICATION_ID);

    this.metricsReporter = new MetricsReporter();
  }

  public void init() {

  }

  public void start() {
    if (metricEnable) {
      metricsReporter.start();
    }
  }

  public void stop() {
    if (metricEnable && stopped.compareAndSet(false, true)) {
      metricsReporter.sendMessage();
      metricsReporter.interrupt();
    }
  }


  public void logRPCStageTime(Request request, RPCTimes rpcTimes) {
    if (metricEnable) {
      PSFTimes time = new PSFTimes();
      int buffLen;
      if(request.getData() instanceof GetUDFRequest) {
        time.funcName = ((GetUDFRequest) request.getData()).getGetFuncClass();
        buffLen = request.bufferLen();
      } else {
        time.funcName = ((UpdateUDFRequest) request.getData()).getUpdaterFuncClass();
        buffLen = request.bufferLen();
      }

      time.reqScheTime = rpcTimes.clientTimes.getChannelStart - rpcTimes.clientTimes.requestStart;
      time.reqWaitChannelTime = rpcTimes.clientTimes.getTokenStart - rpcTimes.clientTimes.getChannelStart;
      time.getTokenTime = rpcTimes.clientTimes.allocateRequestBufStart - rpcTimes.clientTimes.getTokenStart;
      time.reqBufAllocTime = rpcTimes.clientTimes.serializeRequestStart - rpcTimes.clientTimes.allocateRequestBufStart;
      time.reqSerializeTime = rpcTimes.clientTimes.sendRequestStart - rpcTimes.clientTimes.serializeRequestStart;
      time.reqSendTime = rpcTimes.serverTimes.receive - rpcTimes.clientTimes.sendRequestStart;
      time.reqWaitScheTimePS = rpcTimes.serverTimes.handleStart - rpcTimes.serverTimes.receive;
      time.reqDeserializeTime = rpcTimes.serverTimes.partExeStart - rpcTimes.serverTimes.deserializeRequestStart;
      time.handleTime = rpcTimes.serverTimes.allocateResponseBufStart - rpcTimes.serverTimes.partExeStart;
      time.retBufAllocTime = rpcTimes.serverTimes.serilizeResponseStart - rpcTimes.serverTimes.allocateResponseBufStart;
      time.retSerializeTime = rpcTimes.serverTimes.serilizeResponseOver - rpcTimes.serverTimes.serilizeResponseStart;
      time.retSendTime = rpcTimes.clientTimes.recvResponseStart - rpcTimes.serverTimes.sendResponseStart;
      time.retHandleScheTime = rpcTimes.clientTimes.deserializeResponseStart - rpcTimes.clientTimes.recvResponseStart;
      time.retDeserializeTime = rpcTimes.clientTimes.handleResponseStart - rpcTimes.clientTimes.deserializeResponseStart;
      time.bufferLen = buffLen;

      // merge psf time by psf name
      AngelWorkerLoad metrics = new AngelWorkerLoad(time);
      AngelMetrics mergedMetrics = metricsMap.get(metrics.getPsfName());
      if (mergedMetrics == null) {
        mergedMetrics = metricsMap.putIfAbsent(metrics.getPsfName(), createWorkerMetrics(metrics));
        if (mergedMetrics != null) {
          mergedMetrics.merge(metrics);
        }
      } else {
        mergedMetrics.merge(metrics);
      }
    }
  }

  private AngelWorkerMetrics createWorkerMetrics(AngelWorkerLoad metrics ) {
    String containerId = MetricsUtils.getContainerId();
    return new AngelWorkerMetrics("", containerId, metrics);
  }

  public void handleFailedMsg(Request request) {
    if (metricEnable) {
      if (!keyToFailMsgMap.isEmpty()) {
        String funcName = "";
        if (request != null) {
          if(request.getData() instanceof GetUDFRequest) {
            funcName = ((GetUDFRequest) request.getData()).getGetFuncClass();
          } else if (request.getData() instanceof UpdateUDFRequest) {
            funcName = ((UpdateUDFRequest) request.getData()).getUpdaterFuncClass();
          } else {
            funcName = request.getType().name();
          }
        }

        if (metricsMap.containsKey(funcName)) {
          AngelWorkerMetrics metrics = metricsMap.get(funcName);
          metricsMap.replace(funcName, metrics);
        } else {
          String containerId = MetricsUtils.getContainerId();
          AngelWorkerMetrics metrics = new AngelWorkerMetrics("", containerId, new AngelWorkerLoad(funcName));
          metricsMap.put(funcName, metrics);
        }
      }
    }
  }

  /**
   * Report worker metric message to NGCP
   */
  class MetricsReporter extends Thread {

    AngelMetricsReporter reporter;

    @Override
    public void run() {

      try {
        reporter = MetricsUtils.getOrCreateReporter(
            conf.get(AngelConf.ANGEL_METRICS_REPORTER,
                AngelConf.DEFAULT_ANGEL_METRICS_REPORTER));
      } catch (Throwable e) {
        LOG.error("Can not init reporter ", e);
        return;
      }

      while (!stopped.get() && !Thread.interrupted()) {
        try {
          Thread.sleep(reportIntervalMs);

          sendMessage();

        } catch (InterruptedException e) {
          if (!stopped.get()) {
            LOG.error("Reporter is interrupt ", e);
          }
        } catch (Throwable e) {
          LOG.error("Reporter is failed ", e);
        }
      }
    }

    public void sendMessage() {
      // Switch
      ConcurrentHashMap<String, AngelWorkerMetrics> oldMetrics = metricsMap;
      metricsMap = new ConcurrentHashMap<>(oldMetrics.size());

      ConcurrentHashMap<FailedMsg, AtomicInteger> oldFailMsgMap = keyToFailMsgMap;
      keyToFailMsgMap = new ConcurrentHashMap<>(oldMetrics.size());

      String failedMsg = oldFailMsgMap.isEmpty() ? "" : oldFailMsgMap.toString();

      List<String> metrics = new ArrayList<>();
      for (Map.Entry<String, AngelWorkerMetrics> entry : oldMetrics.entrySet()) {
        AngelWorkerMetrics metric = entry.getValue();
        metric.setAppId(appId);
        metric.setPsfFailMsg(failedMsg);
        metrics.add(metric.generateTextMessage(sep));
      }

      if (metrics.size() > 0) {
        reporter.asyncSend(metrics, conf, tid);
      }
    }
  }
}
