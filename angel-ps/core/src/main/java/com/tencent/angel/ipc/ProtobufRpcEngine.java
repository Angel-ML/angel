/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Add shutDown method to fix Angel client exit problem.
 */
package com.tencent.angel.ipc;

import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.tencent.angel.exception.StandbyException;
import com.tencent.angel.exception.UnknownProtocolException;
import com.tencent.angel.io.Addressing;
import com.tencent.angel.io.retry.FailoverInvokerProvider;
import com.tencent.angel.io.retry.RetryPolicies;
import com.tencent.angel.io.retry.RetryPolicy;
import com.tencent.angel.io.retry.RetryPolicy.RetryAction;
import com.tencent.angel.utils.ThreadUtils;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.RPCProtos;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link RpcEngine} implementation for ProtoBuf-based RPCs.
 */
class ProtobufRpcEngine implements RpcEngine {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufRpcEngine.class);
  protected final static ClientCache CLIENTS = new ClientCache();

  @Override
  public VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf, SocketFactory factory,
      int rpcTimeout, List<String> addrList4Failover) throws IOException {
    InvocationHandler invokerHandler = null;
    if (addrList4Failover != null && addrList4Failover.size() > 1) {
      // 创建failoverHandler
      invokerHandler =
          new FailoverInvoker(protocol, addr, conf, factory, rpcTimeout, addrList4Failover);
    } else {
      invokerHandler = new Invoker(protocol, addr, conf, factory, rpcTimeout);
    }
    return (VersionedProtocol) Proxy.newProxyInstance(protocol.getClassLoader(),
        new Class[] {protocol}, invokerHandler);
  }

  @Override
  public void stopProxy(VersionedProtocol proxy) {
    if (proxy != null) {
      InvocationHandler i = Proxy.getInvocationHandler(proxy);
      if (i instanceof Invoker) {
        ((Invoker) i).close();
      }
      if (i instanceof FailoverInvoker) {
        ((FailoverInvoker) i).close();
      }
    }
  }

  @Override
  public Server getServer(Class<? extends VersionedProtocol> protocol, Object instance,
      Class<?>[] ifaces, String bindAddress, int port, Configuration conf) throws IOException {
    return new Server(instance, ifaces, conf, bindAddress, port);
  }

  @Override
  public void shutDown() {
    if(CLIENTS != null) {
      CLIENTS.clear();
    }
  }

  static class FailoverInvoker implements InvocationHandler {
    private final FailoverInvokerProvider<ProtobufRpcEngine.Invoker> failoverProvider;
    private final RetryPolicy failoverPolicy = RetryPolicies.failoverOnNetworkException(
        RetryPolicies.RETRY_FOREVER, -1);
    private Invoker currentInvoker;
    private AtomicLong failoverCount = new AtomicLong(0);

    public FailoverInvoker(Class<? extends VersionedProtocol> protocol, InetSocketAddress addr,
        Configuration conf, SocketFactory factory, int rpcTimeout, List<String> addrList4Failover)
        throws IOException {

      List<ProtobufRpcEngine.Invoker> invoklerList = new ArrayList<ProtobufRpcEngine.Invoker>();
      for (String address : addrList4Failover) {
        String hostname = Addressing.parseHostname(address);
        int port = Addressing.parsePort(address);
        InetSocketAddress hostnameAndPort = null;
        if (addr.getHostName().equals(hostname) && addr.getPort() == port) {// 避免重复创建socket
          hostnameAndPort = addr;
        } else {
          hostnameAndPort = new InetSocketAddress(hostname, port);
        }
        Invoker invoker = new Invoker(protocol, hostnameAndPort, conf, factory, rpcTimeout);
        invoklerList.add(invoker);
      }
      failoverProvider = new FailoverInvokerProvider<ProtobufRpcEngine.Invoker>(invoklerList);
      currentInvoker = invoklerList.get(0);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws ServiceException {
      // The number of times this invocation handler has ever been failed over,
      // before this method invocation attempt. Used to prevent concurrent
      // failed method invocations from triggering multiple failover attempts.
      long markFailoverCount;
      synchronized (failoverProvider) {
        markFailoverCount = failoverCount.longValue();
      }
      try {
        return currentInvoker.invoke(proxy, method, args);
      } catch (Exception e) {
        RetryAction action;
        try {
          action = failoverPolicy.shouldRetry(e, -1, -1, false);
        } catch (Exception e1) {
          throw new ServiceException(e1);
        }

        if (action.action == RetryAction.RetryDecision.FAIL) {
          if (action.reason != null) {
            LOG.warn("Exception while invoking " + proxy.getClass() + "." + method.getName()
                + ". Not retrying because " + action.reason, e);
          }
        } else {
          if (action.delayMillis > 0) {
            ThreadUtils.sleepAtLeastIgnoreInterrupts(action.delayMillis);
          }

          if (action.action == RetryAction.RetryDecision.FAILOVER_AND_RETRY) {
            // Make sure that concurrent failed method invocations
            // only cause a
            // single actual fail over.
            synchronized (failoverProvider) {
              if (markFailoverCount == failoverCount.longValue()) {
                currentInvoker = failoverProvider.performceFailover();
                failoverCount.incrementAndGet();
                LOG.warn("Exception while invoking " + method.getDeclaringClass() + "."
                    + method.getName() + " method failover happens...failoverCount:"
                    + failoverCount.longValue() + "current address is:"
                    + currentInvoker.address.getAddress());
              } else {
                LOG.warn("A failover has occurred since the start of this method"
                    + " invocation attempt.");
              }
            }
          }
        }
        if (e instanceof ServiceException) {
          throw (ServiceException) e;
        }
        throw new ServiceException(e);
      }
    }

    public void close() {
      for (int j = 0; j < failoverProvider.getFailoverList().size(); j++) {
        Invoker i = failoverProvider.getFailoverList().get(j);
        if (i != null) {
          i.close();
        }
      }
    }
  }

  static class Invoker implements InvocationHandler {
    private static final Map<String, Message> returnTypes =
        new ConcurrentHashMap<String, Message>();
    private Class<? extends VersionedProtocol> protocol;
    private InetSocketAddress address;
    private NettyTransceiver client;
    private boolean isClosed = false;
    final private int rpcTimeout;
    private final long clientProtocolVersion;
    private Configuration conf;

    public Invoker(Class<? extends VersionedProtocol> protocol, InetSocketAddress addr,
        Configuration conf, SocketFactory factory, int rpcTimeout) throws IOException {
      this.protocol = protocol;
      this.address = addr;
      this.conf = conf;
      this.client = CLIENTS.getClient(this.address, factory, this.conf);
      this.rpcTimeout = rpcTimeout;

      try {
        this.clientProtocolVersion = MLRPC.getProtocolVersion(protocol);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Exception encountered during " + protocol, e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Exception encountered during " + protocol, e);
      } catch (Exception e) {
        LOG.debug("unspecified Exception: " + e);
        throw new RuntimeException(e);
      }
    }

    private RPCProtos.RpcRequestBody constructRpcRequest(Method method, Object[] params)
        throws ServiceException {
      RPCProtos.RpcRequestBody rpcRequest;
      RPCProtos.RpcRequestBody.Builder builder = RPCProtos.RpcRequestBody.newBuilder();
      builder.setMethodName(method.getName());
      Message param;
      int length = params.length;
      if (length == 2) {
        // RpcController + Message in the method args
        // (generated code from RPC bits in .proto files have
        // RpcController)
        param = (Message) params[1];
      } else if (length == 1) { // Message
        param = (Message) params[0];
      } else {
        throw new ServiceException("Too many parameters for request. Method: [" + method.getName()
            + "]" + ", Expected: 2, Actual: " + params.length);
      }
      builder.setRequestClassName(param.getClass().getName());
      builder.setRequest(param.toByteString());
      builder.setClientProtocolVersion(clientProtocolVersion);
      rpcRequest = builder.build();
      return rpcRequest;
    }

    /**
     * This is the client side invoker of RPC method. It only throws ServiceException, since the
     * invocation proxy expects only ServiceException to be thrown by the method in case protobuf
     * service.
     * 
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered on the client side in this method are set as cause in
     * ServiceException as is.</li>
     * <li>Exceptions from the server are wrapped in RemoteException and are set as cause in
     * ServiceException</li>
     * </ol>
     * 
     * Note that the client calling protobuf RPC methods, must handle ServiceException by getting
     * the cause from the ServiceException. If the cause is RemoteException, then unwrap it to get
     * the exception thrown by the server.
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws ServiceException {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = System.currentTimeMillis();
      }
      Object[] finalArgs = args;
      Callback callback = null;
      Class<? extends VersionedProtocol> finalProtocol = protocol;
      if (args[args.length - 1] instanceof Callback) {
        finalArgs = Arrays.copyOf(args, args.length - 1);
        callback = (Callback<?>) args[args.length - 1];
        finalProtocol = (Class<? extends VersionedProtocol>) protocol.getGenericInterfaces()[0];
      }

      long beforeConstructTs = System.currentTimeMillis();
      RPCProtos.RpcRequestBody rpcRequest = constructRpcRequest(method, finalArgs);
      // LOG.info("method " + method.getName() + "construct request time = "
      // + (System.currentTimeMillis() - beforeConstructTs));

      Message val = null;
      try {
        val = client.call(rpcRequest, finalProtocol, rpcTimeout, callback);

        if (LOG.isDebugEnabled()) {
          long callTime = System.currentTimeMillis() - startTime;
          if (LOG.isTraceEnabled())
            LOG.trace("Call: " + method.getName() + " " + callTime);
        }
        return val;
      } catch (Throwable e) {
        Throwable ex = e;

        if (ex instanceof ServiceException) {
          throw (ServiceException) ex;
        }
        if (ex instanceof ExecutionException) {
          while (ex.getCause() != null) {
            ex = ex.getCause();
            if (ex instanceof StandbyException) {
              throw new ServiceException(ex);
            }
          }
        }
        throw new ServiceException(e);
      }
    }

    synchronized protected void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    static Message getReturnProtoType(Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      }

      Class<?> returnType = method.getReturnType();
      Method newInstMethod = returnType.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      Message protoType = (Message) newInstMethod.invoke(null, (Object[]) null);
      returnTypes.put(method.getName(), protoType);
      return protoType;
    }
  }

  public static class Server extends NettyServer {
    Object instance;
    Class<?> implementation;
    private static final String WARN_RESPONSE_TIME = "ml.ipc.warn.response.time";
    private static final String WARN_RESPONSE_SIZE = "ml.ipc.warn.response.size";

    /** Default value for above params */
    private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
    private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

    private final int warnResponseTime;
    private final int warnResponseSize;

    protected static final Logger TRACELOG = LoggerFactory.getLogger(Server.class);

    private final InetSocketAddress listenerAddress;

    public Server(Object instance, final Class<?>[] ifaces, Configuration conf, String bindAddress,
        int port) throws IOException {
      super(new InetSocketAddress(bindAddress, port), conf);
      this.listenerAddress = new InetSocketAddress(bindAddress, this.getPort());
      this.instance = instance;
      this.implementation = instance.getClass();

      this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME, DEFAULT_WARN_RESPONSE_TIME);
      this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE, DEFAULT_WARN_RESPONSE_SIZE);
    }

    private static final Map<String, Message> methodArg = new ConcurrentHashMap<String, Message>();
    private static final Map<String, Method> methodInstances =
        new ConcurrentHashMap<String, Method>();

    @Override
    /**
     * This is a server side method, which is invoked over RPC. On success
     * the return response has protobuf response payload. On failure, the
     * exception name and the stack trace are returned in the protobuf response.
     */
    public Message call(Class<? extends VersionedProtocol> protocol,
        RPCProtos.RpcRequestBody rpcRequest, long receiveTime) throws IOException {
      try {
        String methodName = rpcRequest.getMethodName();
        Method method = getMethod(protocol, methodName);
        if (method == null) {
          throw new UnknownProtocolException("Method " + methodName + " doesn't exist in protocol "
              + protocol.getName());
        }

        long clientVersion = rpcRequest.getClientProtocolVersion();

        // get an instance of the method arg type
        Message protoType = getMethodArgType(method);
        Message param = protoType.newBuilderForType().mergeFrom(rpcRequest.getRequest()).build();
        Message result;
        Object impl = null;
        if (protocol.isAssignableFrom(this.implementation)) {
          impl = this.instance;
        } else {
          throw new UnknownProtocolException(protocol, "the server class is "
              + this.implementation.getName());
        }

        long startTime = System.currentTimeMillis();
        if (method.getParameterTypes().length == 2) {
          // RpcController + Message in the method args
          // (generated code from RPC bits in .proto files have
          // RpcController)
          result = (Message) method.invoke(impl, null, param);
        } else if (method.getParameterTypes().length == 1) {
          // Message (hand written code usually has only a single
          // argument)
          result = (Message) method.invoke(impl, param);
        } else {
          throw new ServiceException("Too many parameters for method: [" + method.getName() + "]"
              + ", allowed (at most): 2, Actual: " + method.getParameterTypes().length);
        }
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime - receiveTime);

        if (TRACELOG.isDebugEnabled()) {
          TRACELOG.debug(getRemoteAddress() + " Call #" + "; served=" + protocol.getSimpleName()
              + "#" + method.getName() + ", queueTime=" + qTime + ", processingTime="
              + processingTime + ", request=");
        }

        long responseSize = result.getSerializedSize();
        // log any RPC responses that are slower than the configured
        // warn
        // response time or larger than configured warning size
        boolean tooSlow = (processingTime > warnResponseTime && warnResponseTime > -1);
        boolean tooLarge = (responseSize > warnResponseSize && warnResponseSize > -1);
        if (tooSlow || tooLarge) {
          // when tagging, we let TooLarge trump TooSmall to keep
          // output simple
          // note that large responses will often also be slow.
          StringBuilder buffer = new StringBuilder(256);
          buffer.append(methodName);
          buffer.append("(");
          buffer.append(param.getClass().getName());
          buffer.append(")");
          buffer.append(", client version=").append(clientVersion);
          logResponse(new Object[] {rpcRequest.getRequest()}, methodName, buffer.toString(),
              (tooLarge ? "TooLarge" : "TooSlow"), startTime, processingTime, qTime, responseSize);
          // provides a count of log-reported slow responses

        }

        return result;
      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException) target;
        }
        if (target instanceof ServiceException) {
          throw ProtobufUtil.getRemoteException((ServiceException) target);
        }
        IOException ioe = new IOException(target.toString());
        ioe.setStackTrace(target.getStackTrace());
        throw ioe;
      } catch (Throwable e) {
        if (!(e instanceof IOException)) {
          LOG.error("Unexpected throwable object ", e);
        }
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }

    static Method getMethod(Class<? extends VersionedProtocol> protocol, String methodName) {
      Method method = methodInstances.get(methodName);
      if (method != null) {
        return method;
      }
      Method[] methods = protocol.getMethods();
      for (Method m : methods) {
        if (m.getName().equals(methodName)) {
          m.setAccessible(true);
          methodInstances.put(methodName, m);
          return m;
        }
      }
      return null;
    }

    static Message getMethodArgType(Method method) throws Exception {
      Message protoType = methodArg.get(method.getName());
      if (protoType != null) {
        return protoType;
      }

      Class<?>[] args = method.getParameterTypes();
      Class<?> arg;
      if (args.length == 2) {
        // RpcController + Message in the method args
        // (generated code from RPC bits in .proto files have
        // RpcController)
        arg = args[1];
      } else if (args.length == 1) {
        arg = args[0];
      } else {
        // unexpected
        return null;
      }
      // in the protobuf methods, args[1] is the only significant argument
      Method newInstMethod = arg.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      protoType = (Message) newInstMethod.invoke(null, (Object[]) null);
      methodArg.put(method.getName(), protoType);
      return protoType;
    }

    /**
     * Logs an RPC response to the LOG file, producing valid JSON objects for client Operations.
     * 
     * @param params The parameters received in the call.
     * @param methodName The name of the method invoked
     * @param call The string representation of the call
     * @param tag The tag that will be used to indicate this event in the log.
     * @param startTime The time that the call was initiated, in ms.
     * @param processingTime The duration that the call took to run, in ms.
     * @param qTime The duration that the call spent on the queue prior to being initiated, in ms.
     * @param responseSize The size in bytes of the response buffer.
     */
    void logResponse(Object[] params, String methodName, String call, String tag, long startTime,
        int processingTime, int qTime, long responseSize) throws IOException {
      // for JSON encoding
      ObjectMapper mapper = new ObjectMapper();
      // base information that is reported regardless of type of call
      Map<String, Object> responseInfo = new HashMap<String, Object>();
      responseInfo.put("starttimems", startTime);
      responseInfo.put("processingtimems", processingTime);
      responseInfo.put("queuetimems", qTime);
      responseInfo.put("responsesize", responseSize);
      responseInfo.put("class", instance.getClass().getSimpleName());
      responseInfo.put("method", methodName);
      responseInfo.put("call", call);
      /*
       * LOG.warn("(response" + tag + "): " + mapper.writeValueAsString(responseInfo));
       */
    }

    protected static void log(String value, Logger LOG) {
      String v = value;
      if (v != null && v.length() > 55)
        v = v.substring(0, 55) + "...";
      LOG.info(v);
    }

    @Override
    public InetSocketAddress getListenerAddress() {
      return this.listenerAddress;
    }
  }
}
