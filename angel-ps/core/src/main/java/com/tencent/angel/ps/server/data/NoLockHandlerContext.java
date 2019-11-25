package com.tencent.angel.ps.server.data;

import com.tencent.angel.ps.server.data.ChannelHandlerContextMsg;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class NoLockHandlerContext {
  private final LinkedBlockingQueue<ChannelHandlerContextMsg> msgQueue;
  private final Thread handler;

  private final AtomicBoolean stopped;

  public NoLockHandlerContext() {
    msgQueue = new LinkedBlockingQueue();
    handler = new Processor();
    stopped = new AtomicBoolean(false);
  }

  class Processor extends Thread {
    @Override
    public void run() {
      while(!stopped.get() && !Thread.interrupted()) {
        try {
          ChannelHandlerContextMsg msg = msgQueue.take();

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}


