package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.ParameterServerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RPCContext {
  private static final Log LOG = LogFactory.getLog(RPCContext.class);
  /** max number of the flight requests */
  private final AtomicInteger maxInflightRPCNum;

  /** total flight request counter */
  private final AtomicInteger inflightRPCCounter;

  /** max number of the flight requests to each ps */
  private final AtomicInteger maxInflightRPCNumPerServer;

  /** ps id to flight request counter map */
  private final ConcurrentHashMap<ParameterServerId, AtomicInteger> serverInflightRPCCounters;

  private final AtomicInteger lastOOMInflightRPCCounter;

  public RPCContext() {
    maxInflightRPCNum = new AtomicInteger(0);
    inflightRPCCounter = new AtomicInteger(0);
    maxInflightRPCNumPerServer = new AtomicInteger(0);
    serverInflightRPCCounters = new ConcurrentHashMap<>();
    lastOOMInflightRPCCounter = new AtomicInteger(0);
  }

  public void init(Configuration conf, ParameterServerId[] psIds) {
    int serverNum =
      conf.getInt(AngelConf.ANGEL_PS_NUMBER,
        AngelConf.DEFAULT_ANGEL_PS_NUMBER);

    maxInflightRPCNumPerServer.set(conf.getInt(
      AngelConf.ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER,
      AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER));

    for (int i = 0; i < psIds.length; i++) {
      serverInflightRPCCounters.put(psIds[i], new AtomicInteger(0));
    }

    int maxReqNumInFlight =
      conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_MAX);

    if (maxReqNumInFlight > serverNum * maxInflightRPCNumPerServer.get()) {
      maxReqNumInFlight = serverNum * maxInflightRPCNumPerServer.get();
    }
    this.maxInflightRPCNum.set(maxReqNumInFlight);
  }

  public void before(ParameterServerId psId) {
    inflightRPCCounter.incrementAndGet();
    serverInflightRPCCounters.get(psId).incrementAndGet();
  }

  public void after(ParameterServerId psId) {
    inflightRPCCounter.decrementAndGet();
    serverInflightRPCCounters.get(psId).decrementAndGet();
  }

  public void oom() {
    lastOOMInflightRPCCounter.set(inflightRPCCounter.get());
    maxInflightRPCNum.set((int)(maxInflightRPCNum.get() * 0.8));
  }

  public int getServerInflightRPCCounters(ParameterServerId psId) {
    return serverInflightRPCCounters.get(psId).get();
  }

  public int getInflightRPCCounters() {
    return inflightRPCCounter.get();
  }

  public int getMaxInflightRPCNum() {
    return maxInflightRPCNum.get();
  }

  public int getMaxInflightRPCNumPerServer() {
    return maxInflightRPCNumPerServer.get();
  }


  public void print() {
    LOG.info("maxInflightRPCNum=" + maxInflightRPCNum.get());
    LOG.info("maxInflightRPCNumPerServer=" + maxInflightRPCNumPerServer.get());
    LOG.info("inflightRPCCounter=" + inflightRPCCounter.get());
    LOG.info("lastOOMInflightRPCCounter=" + lastOOMInflightRPCCounter.get());
    for (Map.Entry<ParameterServerId, AtomicInteger> entry : serverInflightRPCCounters.entrySet()) {
      LOG.info("for server " + entry.getKey() + " inflight rpc number=" + entry.getValue().get());
    }
  }
}
