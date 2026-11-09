package com.tencent.angel.metrics;

import java.util.List;
import org.apache.hadoop.conf.Configuration;

public interface AngelMetricsReporter {

  //send metrics to the configured metrics backend
  void send(String message, String tid);

  //send metrics to the configured metrics backend
  void send(List<String> message, String tid);

  //send metrics to the configured metrics backend
  void send(String message, Configuration conf, String tid);

  void send(List<String> message, Configuration conf, String tid);

  //send metrics to the configured metrics backend
  void asyncSend(String message, String tid);

  //send metrics to the configured metrics backend
  void asyncSend(List<String> message, String tid);

  //send metrics to the configured metrics backend
  void asyncSend(String message, Configuration conf, String tid);

  void asyncSend(List<String> message, Configuration conf, String tid);

  void close();
}

