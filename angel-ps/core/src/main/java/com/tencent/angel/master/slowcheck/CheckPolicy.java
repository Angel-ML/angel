package com.tencent.angel.master.slowcheck;

/**
 * Base class of slow ps/worker check policy
 */
public abstract class CheckPolicy implements CheckPolicyInterface {
  /**
   * Create a CheckPolicy
   */
  public CheckPolicy() {}
}
