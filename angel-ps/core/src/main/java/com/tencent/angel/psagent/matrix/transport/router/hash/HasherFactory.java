package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.psagent.matrix.transport.router.KeyHash;
import java.util.concurrent.ConcurrentHashMap;

public class HasherFactory {
  private static final ConcurrentHashMap<Class, KeyHash> hasherCache = new ConcurrentHashMap<>();

  public static KeyHash getHasher(Class hasherClass) {
    KeyHash hasher = hasherCache.get(hasherClass);
    if(hasher == null) {
      try {
        KeyHash newHasher = (KeyHash) hasherClass.newInstance();
        hasher = hasherCache.putIfAbsent(hasherClass, newHasher);
        if(hasher == null) {
          hasher = hasherCache.get(hasherClass);
        }
      } catch (Throwable e) {
        throw new RuntimeException("Can not init hash class " + hasherClass, e);
      }
    }

    return hasher;
  }
}
