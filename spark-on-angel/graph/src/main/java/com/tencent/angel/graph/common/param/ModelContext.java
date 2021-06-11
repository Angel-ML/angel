package com.tencent.angel.graph.common.param;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.graph.common.conf.AngelGraphConf;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;

public class ModelContext implements Serializable {
  private final int partitionNum;
  private final long minNodeId;
  private final long maxNodeId;
  private final long nodeNum;
  private final String modelName;
  private final Properties conf;

  public ModelContext(int partitionNum, long minNodeId, long maxNodeId, long nodeNum, String modelName,
      Configuration hadoopConf) {
    this.partitionNum = partitionNum;
    this.minNodeId = minNodeId;
    this.maxNodeId = maxNodeId;
    this.nodeNum = nodeNum;
    this.modelName = modelName;
    this.conf = new Properties();
    Iterator<Entry<String, String>> iter = hadoopConf.iterator();
    while(iter.hasNext()) {
      Entry<String, String> kv = iter.next();
      conf.setProperty(kv.getKey(), kv.getValue());
    }
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public long getMinNodeId() {
    return minNodeId;
  }

  public long getMaxNodeId() {
    return maxNodeId;
  }

  public String getModelName() {
    return modelName;
  }

  public boolean isUseHashPartition() {
    return "hash".equalsIgnoreCase(
        conf.getProperty(AngelConf.ANGEL_PS_ROUTER_TYPE, AngelConf.DEFAULT_ANGEL_PS_ROUTER_TYPE));
  }

  public boolean isUseRangePartition() {
    return "range".equalsIgnoreCase(
        conf.getProperty(AngelConf.ANGEL_PS_ROUTER_TYPE, AngelConf.DEFAULT_ANGEL_PS_ROUTER_TYPE));
  }

  public boolean isLoadBalancePartitionEnable() {
    return getBoolean(AngelConf.ANGEL_PS_LOADBALANCE_PARTITION_ENABLE,
        AngelConf.DEFAULT_ANGEL_PS_LOADBALANCE_PARTITION_ENABLE);
  }

  public boolean isUseBytesFormatForReadOnly() {
    return getBoolean(AngelGraphConf.ANGEL_GRAPH_USE_BYTES_FOR_READONLY,
        AngelGraphConf.DEFAULT_ANGEL_GRAPH_USE_BYTES_FOR_READONLY);
  }

  private boolean getBoolean(String key, boolean defaultValue) {
    String value = conf.getProperty(key);
    if(key == null) {
      return defaultValue;
    }

    return "true".equalsIgnoreCase(value);
  }

  private int getInt(String key, int defaultValue) {
    String value = conf.getProperty(key);
    if(key == null) {
      return defaultValue;
    }

    return Integer.valueOf(value);
  }


  public void setMasterHost(String masterHost) {
    conf.setProperty(AngelConf.ANGEL_AM_HOST, masterHost);
  }

  public void setMasterPort(int masterPort) {
    conf.setProperty(AngelConf.ANGEL_AM_PORT, "" + masterPort);
  }

  public String getMasterHost() {
    return conf.getProperty(AngelConf.ANGEL_AM_HOST);
  }

  public int getMasterPort() {
    return getInt(AngelConf.ANGEL_AM_PORT, -1);
  }

  public Configuration getHadoopConf() {
    Configuration hadoopConf = new Configuration();
    Set<String> keyNames = conf.stringPropertyNames();
    for(String keyName : keyNames) {
      hadoopConf.set(keyName, conf.getProperty(keyName));
    }
    return hadoopConf;
  }

  public long getNodeNum() {
    return nodeNum;
  }

  public Properties getConf() {
    return conf;
  }
}
