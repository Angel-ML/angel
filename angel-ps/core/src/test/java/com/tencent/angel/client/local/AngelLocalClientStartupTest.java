package com.tencent.angel.client.local;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.localcluster.LocalClusterContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AngelLocalClientStartupTest {
  private static class TestClient extends AngelLocalClient {
    TestClient() {
      super(new Configuration(false));
    }

    void awaitMaster(int seconds) throws Exception {
      updateMaster(seconds);
    }
  }

  @After public void clearContext() {
    LocalClusterContext.get().clear();
  }

  @Test(timeout = 5000) public void propagatesStartupLinkageFailure() throws Exception {
    NoClassDefFoundError cause = new NoClassDefFoundError("missing-test-dependency");
    LocalClusterContext.get().setStartupFailure(cause);
    try {
      new TestClient().awaitMaster(60);
      fail("Startup failure must not leave the client waiting");
    } catch (AngelException failure) {
      assertSame(cause, failure.getCause());
    }
  }

  @Test(timeout = 5000) public void reportsMasterStartupTimeout() throws Exception {
    LocalClusterContext.get().clear();
    try {
      new TestClient().awaitMaster(0);
      fail("Missing master must cause a timeout failure");
    } catch (AngelException failure) {
      assertTrue(failure.getMessage().contains("Timed out"));
    }
  }
}
