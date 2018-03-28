package com.tencent.angel.psagent.client;

import com.tencent.angel.common.location.Location;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class PSControlClientManager {
  /** PS location to control rpc client */
  private final ConcurrentHashMap<Location, PSControlClient> pss = new ConcurrentHashMap<>();

  public PSControlClient getOrCreatePSClient(Location loc) throws IOException {
    PSControlClient ps = pss.get(loc);
    if(ps == null) {
      ps = pss.putIfAbsent(loc, createPSControlClient(loc));
      if(ps == null) {
        ps = pss.get(loc);
      }
    }
    return ps;
  }

  private PSControlClient createPSControlClient(Location loc) throws IOException {
    PSControlClient client = new PSControlClient();
    client.init(loc);
    return client;
  }
}
