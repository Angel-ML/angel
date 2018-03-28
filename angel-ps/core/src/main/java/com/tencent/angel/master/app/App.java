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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.master.app;

import com.tencent.angel.RunningMode;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetJobReportResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.JobReportProto;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.JobStateProto;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Angel Application state machine.
 */
public class App extends AbstractService implements EventHandler<AppEvent> {
  private static final Log LOG = LogFactory.getLog(App.class);
  private final static String JOB_STATE_PREFIX = "J_";

  private final AMContext context;

  /** application diagnostics */
  private final List<String> diagnostics;

  /** application state machine */
  private final StateMachine<AppState, AppEventType, AppEvent> stateMachine;

  /** the state is externally enforced, its priority is higher than the state of the state machine */
  private AppState forcedState = null;

  /** application launch time */
  private final long launchTime;

  /** application finish time */
  private long finishTime;

  /** read/write lock */
  private final Lock readLock;
  private final Lock writeLock;

  /** identify whether the application needs to be retried, */
  private boolean shouldRetry;
  
  /** state timeout monitor*/
  private Thread stateMonitor;
  
  /** state to state start timestamp map*/
  private final Map<AppState, Long> stateToTsMap;
  
  /**the longest time in milliseconds for a state(NEW,INITED,EXECUTE_SUCCESSED,SUCCEEDED,FAILED,KILLED)*/
  private final long stateTimeOutMs;
  
  private final AtomicBoolean stopped;

  public App(AMContext context) {
    super(App.class.getName());
    this.context = context;
    stateMachine = stateMachineFactory.make(this);
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.launchTime = context.getStartTime();
    shouldRetry = false;
    diagnostics = new ArrayList<String>();
    
    stateTimeOutMs = 
        context.getConf().getLong(AngelConf.ANGEL_AM_APPSTATE_TIMEOUT_MS,
            AngelConf.DEFAULT_ANGEL_AM_APPSTATE_TIMEOUT_MS);
    stateToTsMap = new HashMap<AppState, Long>();
    stateToTsMap.put(AppState.NEW, context.getClock().getTime());
    stopped = new AtomicBoolean(false);
  }

  private static final AppSuccessTransition APP_SUCCESS_TRANSITION = new AppSuccessTransition();
  private static final AppKilledTransition APP_KILLED_TRANSITION = new AppKilledTransition();
  private static final AppFailedTransition APP_FAILED_TRANSITION = new AppFailedTransition();

  protected static final StateMachineFactory<App, AppState, AppEventType, AppEvent> stateMachineFactory =
      new StateMachineFactory<App, AppState, AppEventType, AppEvent>(AppState.NEW)
          .addTransition(AppState.NEW, AppState.INITED, AppEventType.INIT)
          .addTransition(AppState.NEW, AppState.KILLED, AppEventType.KILL, APP_KILLED_TRANSITION)
          .addTransition(AppState.NEW, AppState.FAILED, AppEventType.INTERNAL_ERROR,
              APP_FAILED_TRANSITION)

          .addTransition(AppState.INITED, AppState.RUNNING, AppEventType.START,
              new AppStartTransition())
          .addTransition(AppState.INITED, AppState.KILLED, AppEventType.KILL, APP_KILLED_TRANSITION)
          .addTransition(AppState.INITED, AppState.FAILED, AppEventType.INTERNAL_ERROR,
              APP_FAILED_TRANSITION)

          .addTransition(AppState.RUNNING, AppState.EXECUTE_SUCCESSED, AppEventType.EXECUTE_SUCESS)
          .addTransition(AppState.RUNNING, AppState.KILLED, AppEventType.KILL,
              APP_KILLED_TRANSITION)
          .addTransition(AppState.RUNNING, AppState.FAILED, AppEventType.INTERNAL_ERROR,
              APP_FAILED_TRANSITION)
          .addTransition(AppState.RUNNING, AppState.SUCCEEDED, AppEventType.SUCCESS,
              APP_SUCCESS_TRANSITION)
          .addTransition(AppState.RUNNING, AppState.COMMITTING, AppEventType.COMMIT)

          .addTransition(AppState.EXECUTE_SUCCESSED, AppState.COMMITTING, AppEventType.COMMIT)
          .addTransition(AppState.EXECUTE_SUCCESSED, AppState.KILLED, AppEventType.KILL,
              APP_KILLED_TRANSITION)
          .addTransition(AppState.EXECUTE_SUCCESSED, AppState.FAILED, AppEventType.INTERNAL_ERROR,
              APP_FAILED_TRANSITION)
          .addTransition(AppState.EXECUTE_SUCCESSED, AppState.SUCCEEDED, AppEventType.SUCCESS,
              APP_SUCCESS_TRANSITION)

          .addTransition(AppState.COMMITTING, AppState.SUCCEEDED, AppEventType.SUCCESS,
              APP_SUCCESS_TRANSITION)
          .addTransition(AppState.COMMITTING, AppState.FAILED, AppEventType.INTERNAL_ERROR,
              APP_FAILED_TRANSITION)
          .addTransition(AppState.COMMITTING, AppState.KILLED, AppEventType.KILL,
              APP_KILLED_TRANSITION)

          .addTransition(
              AppState.SUCCEEDED,
              AppState.SUCCEEDED,
              EnumSet.of(AppEventType.INIT, AppEventType.START, AppEventType.COMMIT, AppEventType.EXECUTE_SUCESS,
                  AppEventType.SUCCESS, AppEventType.KILL, AppEventType.INTERNAL_ERROR))

          .addTransition(
              AppState.KILLED,
              AppState.KILLED,
              EnumSet.of(AppEventType.INIT, AppEventType.START, AppEventType.COMMIT, AppEventType.EXECUTE_SUCESS,
                  AppEventType.SUCCESS, AppEventType.KILL, AppEventType.INTERNAL_ERROR))

          .addTransition(
              AppState.FAILED,
              AppState.FAILED,
              EnumSet.of(AppEventType.INIT, AppEventType.START, AppEventType.COMMIT, AppEventType.EXECUTE_SUCESS,
                  AppEventType.SUCCESS, AppEventType.KILL, AppEventType.INTERNAL_ERROR));

  @SuppressWarnings("unchecked")
  public void startExecute() {
    context.getEventHandler().handle(new AppEvent(AppEventType.INIT));
    context.getEventHandler().handle(new AppEvent(AppEventType.START));
  }
  
  /**
   * get state of application, only return RUNNING, EXECUTE_SUCCEEDED, COMMITING, SUCCEEDED, KILLED, FAILED
   * 
   * @return AppState the state of application
   */
  public AppState getExternAppState() {
    AppState state = getInternalState();
    switch (state) {
      case NEW:
      case INITED:
      case RUNNING:
        return AppState.RUNNING;       
      default:
        return state;
    }
  }
  
  @Override
  protected void serviceStart() throws Exception {
    stateMonitor = new Thread(new Runnable() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {
        while(!stopped.get() && !Thread.interrupted()) {
          AppState state = getInternalState();
          try {
            readLock.lock();
            if (stateToTsMap.containsKey(state)
                && context.getClock().getTime() - stateToTsMap.get(state) >= stateTimeOutMs) {
              context.getEventHandler().handle(
                  new InternalErrorEvent(context.getApplicationId(), "app in state " + state
                      + " over " + stateTimeOutMs + " milliseconds!"));
            }
          } finally {
            readLock.unlock();
          }
        }
      }
      
    });
    stateMonitor.setName("app-state-monitor");
    stateMonitor.start();
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (!stopped.getAndSet(true)) {
      if (stateMonitor != null) {
        stateMonitor.interrupt();
        try {
          stateMonitor.join();
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException while stopping", ie);
        }
        stateMonitor = null;
      }
    }
    
    super.serviceStop();
  }

  /**
   * get application running state: current state, current iteration and diagnostics TODO:we can add
   * more details of the app.
   * 
   * @return GetJobReportResponse application state
   */
  public GetJobReportResponse getJobReportResponse() {
    GetJobReportResponse.Builder getJobReportResBuilder = GetJobReportResponse.newBuilder();
    JobReportProto.Builder report = JobReportProto.newBuilder();
    report.setJobState(convertToProtoFormat(getExternAppState()));
    StringBuilder sb = new StringBuilder();
    sb.append(StringUtils.join("\n", getDiagnostics()));
    report.setDiagnostics(sb.toString());

    int totalIteration =
        context.getConf().getInt(AngelConf.ANGEL_TASK_ITERATION_NUMBER,
            AngelConf.DEFAULT_ANGEL_TASK_ITERATION_NUMBER);
    report.setTotalIteration(totalIteration);
    int curIteration = 0;
    if(context.getAlgoMetricsService() != null) {
      curIteration = context.getAlgoMetricsService().getCurrentIter();
      Map<String, String> metrics = context.getAlgoMetricsService().getAlgoMetrics(curIteration);
      MLProtos.Pair.Builder pairBuilder = MLProtos.Pair.newBuilder();

      if(metrics != null) {
        for(Map.Entry<String, String> entry:metrics.entrySet()) {
          report.addMetrics(pairBuilder.setKey(entry.getKey()).setValue(String.valueOf(entry.getValue())).build());
        }
      }
    }
    report.setCurIteration(curIteration);

    getJobReportResBuilder.setJobReport(report);
    return getJobReportResBuilder.build();
  }

  private String toString(Map<String, Double> metrics){
    StringBuilder sb = new StringBuilder();

    for(Map.Entry<String, Double> entry:metrics.entrySet()) {
      sb.append("index name=").append(entry.getKey()).append(",").append("value=").append(entry.getValue());
    }

    return sb.toString();
  }


  /**
   * write application state to output stream
   * 
   * @param out output stream
   */
  public void serilize(FSDataOutputStream out) throws IOException {
    GetJobReportResponse jobState = getJobReportResponse();
    jobState.writeTo(out);
    LOG.info("write app report to file successfully " + jobState);
  }

  private JobStateProto convertToProtoFormat(AppState appState) {
    return JobStateProto.valueOf(JOB_STATE_PREFIX + appState.name());
  }

  public void addDiagnostics(String message) {
    try {
      writeLock.lock();
      diagnostics.add(message);
    } finally {
      writeLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(AppEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing AppEvent type " + event);
    }
    try {
      writeLock.lock();
      AppState oldState = getInternalState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        context.getEventHandler().handle(
            new InternalErrorEvent(context.getApplicationId(),
                "Can't handle this event at current state" + e.getMessage()));
      }
      // notify the event handler of state change
      AppState newState = getInternalState();
      if (oldState != newState) {
        // If new state is not RUNNING and COMMITTING, add it to state timeout monitor
        if(newState != AppState.RUNNING && newState != AppState.COMMITTING){
          stateToTsMap.put(newState, context.getClock().getTime());
        }
        LOG.info(context.getApplicationId() + "Job Transitioned from " + oldState + " to "
            + newState);
      }
    }

    finally {
      writeLock.unlock();
    }
  }

  /**
   * get application state
   * 
   * @return AppState application state
   */
  public AppState getInternalState() {
    readLock.lock();
    try {
      // if forcedState is set, just return
      if (forcedState != null) {
        return forcedState;
      }

      // else get state from state machine
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get application diagnostics
   * 
   * @return List<String> application diagnostics
   */
  public List<String> getDiagnostics() {
    try {
      readLock.lock();
      List<String> cloneDiagnostics = new ArrayList<String>();
      cloneDiagnostics.addAll(diagnostics);
      return cloneDiagnostics;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get application launch time
   * 
   * @return long application launch time, in milliseconds
   */
  public long getLaunchTime() {
    return launchTime;
  }

  /**
   * get application finish time
   * 
   * @return long application finish time, in milliseconds
   */
  public long getFinishTime() {
    try {
      readLock.lock();
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  private void setFinishTime() {
    this.finishTime = context.getClock().getTime();
  }

  /**
   * force the application to specific state
   * 
   * @param state forced state need to set
   */
  public void forceState(AppState state) {
    try {
      writeLock.lock();
      forcedState = state;
      if ((state == AppState.SUCCEEDED) || (state == AppState.FAILED) || (state == AppState.KILLED)) {
        setFinishTime();
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * check application is finish or not. when the application is in the SUCCEEDED, FAILED or KILLED
   * state, indicating that the application is finish
   * 
   * @return boolean true if finish, otherwise false
   */
  public boolean isFinish() {
    AppState state = getInternalState();
    return (state == AppState.SUCCEEDED) || (state == AppState.FAILED)
        || (state == AppState.KILLED);
  }

  /**
   * check application is finish with SUCCEEDED
   * 
   * @return boolean true if finish with SUCCEEDED, otherwise false
   */
  public boolean isSuccess() {
    AppState state = getInternalState();
    return state == AppState.SUCCEEDED;
  }

  /**
   * check if the application master needs to retry
   * 
   * @return boolean true if need retry, otherwise false
   */
  public boolean isShouldRetry() {
    try {
      readLock.lock();
      return shouldRetry;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * set application master retry flag
   * 
   * @param retryEnable retry flag
   */
  public void shouldRetry(boolean retryEnable) {
    try {
      writeLock.lock();
      shouldRetry = retryEnable;
    } finally {
      writeLock.unlock();
    }
  }

  public static class AppSuccessTransition implements SingleArcTransition<App, AppEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(App app, AppEvent event) {
      app.context.getEventHandler().handle(
          new AppFinishEvent(AppFinishEventType.SUCCESS_FINISH, app.context.getApplicationId()));
      app.setFinishTime();
    }
  }

  public static class AppStartTransition implements SingleArcTransition<App, AppEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(App app, AppEvent event) {
      if (app.context.getRunningMode() == RunningMode.ANGEL_PS_WORKER) {
        app.context.getWorkerManager().startAllWorker();
      }
    }
  }

  public static class AppKilledTransition implements SingleArcTransition<App, AppEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(App app, AppEvent event) {
      app.context.getEventHandler().handle(
          new AppFinishEvent(AppFinishEventType.KILL_FINISH, app.context.getApplicationId()));
      app.setFinishTime();
    }
  }

  public static class AppFailedTransition implements SingleArcTransition<App, AppEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(App app, AppEvent event) {
      app.context.getEventHandler().handle(
          new AppFinishEvent(AppFinishEventType.INTERNAL_ERROR_FINISH, app.context
              .getApplicationId()));

      InternalErrorEvent errorEvent = (InternalErrorEvent) event;
      app.shouldRetry = errorEvent.isShouldRetry();

      LOG.info("some error happened, " + errorEvent);
      app.diagnostics.add(errorEvent.getErrorMsg());
      app.setFinishTime();
    }
  }
}
