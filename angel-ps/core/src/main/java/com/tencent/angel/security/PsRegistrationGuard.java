package com.tencent.angel.security;

import com.tencent.angel.common.location.Location;

/**
 * Rules applied to PS registrations arriving on the Master RPC interface.
 *
 * <p>A PS attempt is allowed to repeat the same registration (the heartbeat path resends
 * PSCOMMAND_REGISTER), but it must never be able to replace an address that was already
 * advertised to clients.</p>
 */
public final class PsRegistrationGuard {
  private PsRegistrationGuard() {
  }

  public static boolean conflicts(Location registered, Location requested) {
    if (registered == null || requested == null) {
      return false;
    }
    return !(registered.getIp().equals(requested.getIp())
        && registered.getPort() == requested.getPort());
  }

  public static boolean sourceMatches(String remoteAddress, Location advertised,
      boolean strict) {
    if (!strict) {
      return true;
    }
    String remoteIp = extractRemoteIp(remoteAddress);
    return remoteIp == null || sameHost(remoteIp, advertised.getIp());
  }

  public static String extractRemoteIp(String remote) {
    if (remote == null || remote.trim().isEmpty()) {
      return null;
    }
    String value = remote.trim();
    if (value.startsWith("/")) {
      value = value.substring(1);
    }
    int colon = value.lastIndexOf(':');
    if (colon > 0) {
      value = value.substring(0, colon);
    }
    return value.isEmpty() ? null : value;
  }

  private static boolean sameHost(String left, String right) {
    if (left == null || right == null) {
      return false;
    }
    return left.equals(right) || (isLoopback(left) && isLoopback(right));
  }

  private static boolean isLoopback(String host) {
    return "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host)
        || "0:0:0:0:0:0:0:1".equals(host) || "::1".equals(host);
  }
}
