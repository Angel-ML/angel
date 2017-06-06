

package com.tencent.angel.ipc;

import java.io.IOException;

public class ServerNotRunningYetException extends IOException {

  private static final long serialVersionUID = -1037907015006723006L;

  public ServerNotRunningYetException(String s) {
    super(s);
  }
}
