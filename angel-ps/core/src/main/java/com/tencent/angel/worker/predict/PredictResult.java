 
package com.tencent.angel.worker.predict;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class PredictResult {
  protected static String separator = ",";

  public abstract void writeText(DataOutputStream output) throws IOException;
}
