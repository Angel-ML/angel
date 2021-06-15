package com.tencent.angel.psagent.matrix.transport.router;

import com.tencent.angel.common.Serialize;

public interface IDataPart extends Serialize {
  int size();
}
