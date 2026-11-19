package com.tencent.angel.security;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ipc.VersionedProtocol;
import com.tencent.angel.model.output.format.MatrixFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Collection;

/**
 * Restricts the class names that arrive in RPC headers and model load contexts.
 *
 * <p>Both values used to be fed directly into {@code Class.forName}, which allowed a caller to
 * load arbitrary classes from the server classpath. Angel only needs VersionedProtocol
 * interfaces and concrete MatrixFormat implementations from its own packages; extra classes can
 * be registered explicitly through configuration.</p>
 */
public final class RpcClassGuard {
  private static final String ANGEL_PACKAGE = "com.tencent.angel.";

  private RpcClassGuard() {
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends VersionedProtocol> resolveProtocol(String className,
      Configuration conf) throws ClassNotFoundException {
    if (className == null || className.isEmpty()) {
      throw new ClassNotFoundException("empty RPC protocol name");
    }
    if (!className.startsWith(ANGEL_PACKAGE) && !isExplicitlyAllowed(className, conf,
        AngelConf.ANGEL_RPC_ALLOWED_PROTOCOLS)) {
      throw new ClassNotFoundException("RPC protocol " + className + " is not allowed");
    }
    Class<?> clazz = Class.forName(className);
    if (!clazz.isInterface() || !VersionedProtocol.class.isAssignableFrom(clazz)) {
      throw new ClassNotFoundException(
          "RPC protocol " + className + " is not a VersionedProtocol interface");
    }
    return (Class<? extends VersionedProtocol>) clazz;
  }

  public static void checkFormatClass(String className, Configuration conf) throws IOException {
    if (className == null || className.isEmpty()) {
      throw new IOException("empty model format class name");
    }
    if (!className.startsWith(ANGEL_PACKAGE) && !isExplicitlyAllowed(className, conf,
        AngelConf.ANGEL_MODEL_FORMAT_ALLOWED_CLASSES)) {
      throw new IOException("model format class " + className + " is not allowed");
    }
    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    if (!MatrixFormat.class.isAssignableFrom(clazz) || clazz.isInterface()
        || Modifier.isAbstract(clazz.getModifiers())) {
      throw new IOException("model format class " + className
          + " is not a concrete MatrixFormat implementation");
    }
  }

  private static boolean isExplicitlyAllowed(String className, Configuration conf, String key) {
    if (conf == null) {
      return false;
    }
    Collection<String> values = conf.getTrimmedStringCollection(key);
    return values.contains(className);
  }
}
