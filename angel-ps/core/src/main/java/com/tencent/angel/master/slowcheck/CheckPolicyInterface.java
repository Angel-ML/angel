package com.tencent.angel.master.slowcheck;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.app.AMContext;

import java.util.List;

/**
 * Slow ps/worker check policy interface.
 */
public interface CheckPolicyInterface {
  /**
   * Check slow ps/workers
   * @param context application context
   * @return slow pss/workers
   */
  List<Id> check(AMContext context);
}
