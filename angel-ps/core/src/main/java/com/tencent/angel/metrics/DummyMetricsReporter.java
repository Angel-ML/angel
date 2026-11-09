package com.tencent.angel.metrics;

import java.util.List;
import org.apache.hadoop.conf.Configuration;

public class DummyMetricsReporter implements AngelMetricsReporter {

  @Override
  public void send(String message, String tid) {

  }

  @Override
  public void send(List<String> message, String tid) {

  }

  @Override
  public void send(String message, Configuration conf, String tid) {

  }

  @Override
  public void send(List<String> message, Configuration conf, String tid) {

  }

  @Override
  public void asyncSend(String message, String tid) {

  }

  @Override
  public void asyncSend(List<String> message, String tid) {

  }

  @Override
  public void asyncSend(String message, Configuration conf, String tid) {

  }

  @Override
  public void asyncSend(List<String> message, Configuration conf, String tid) {

  }

  @Override
  public void close() {

  }
}
