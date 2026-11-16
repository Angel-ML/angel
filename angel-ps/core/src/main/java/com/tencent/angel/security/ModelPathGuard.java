package com.tencent.angel.security;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Validation for model paths that arrive through the Master RPC layer.
 *
 * <p>The Master RPC interface is reachable by any client that knows the Master address, so a
 * save/load path supplied in a request must never be used verbatim. This guard normalizes the
 * path, rejects traversal, optionally restricts it to configured roots, and prevents the
 * recursive deletion of directories that were not produced by Angel.</p>
 */
public final class ModelPathGuard {
  private static final Logger LOG = LoggerFactory.getLogger(ModelPathGuard.class);
  public static final String OWNER_FILE = "_angel_model_owner";

  private ModelPathGuard() {
  }

  /**
   * Validate and normalize a client supplied model path.
   *
   * @return the normalized path that must be used by all downstream save/load code
   */
  public static Path validate(Configuration conf, String rawPath, String operation)
      throws IOException {
    if (rawPath == null || rawPath.trim().isEmpty()) {
      throw new IOException(operation + " model path is empty");
    }
    String trimmed = rawPath.trim();
    rejectParentTraversal(trimmed, operation);

    Path requested;
    try {
      requested = new Path(trimmed);
    } catch (IllegalArgumentException e) {
      throw new IOException("invalid " + operation + " model path: " + e.getMessage(), e);
    }

    URI uri = requested.toUri();
    String pathPart = uri.getPath();
    if (pathPart == null || pathPart.isEmpty()) {
      throw new IOException(operation + " model path has no directory component");
    }
    if ("file".equalsIgnoreCase(uri.getScheme()) && uri.getAuthority() != null
        && !uri.getAuthority().isEmpty()) {
      throw new IOException(
          operation + " model path uses an unsupported file authority: " + uri.getAuthority());
    }

    Path normalized = new Path(uri.normalize());
    List<Path> roots = allowedRoots(conf);
    if (!roots.isEmpty() && !isUnderAnyRoot(normalized, roots)) {
      throw new IOException(operation + " model path " + normalized
          + " is outside the configured allowed roots ("
          + AngelConf.ANGEL_MODEL_PATH_ALLOWED_ROOTS + ")");
    }
    return normalized;
  }

  /** Hadoop Path normalizes ".." while parsing, so inspect the raw request string first. */
  private static void rejectParentTraversal(String raw, String operation) throws IOException {
    int schemeEnd = raw.indexOf("://");
    String rest = schemeEnd >= 0 ? raw.substring(schemeEnd + 3) : raw;
    int firstSlash = rest.indexOf('/');
    if (firstSlash >= 0) {
      rest = rest.substring(firstSlash);
    }
    for (String segment : rest.split("/")) {
      if ("..".equals(segment)) {
        throw new IOException(
            operation + " model path contains parent traversal: " + raw);
      }
    }
  }

  private static List<Path> allowedRoots(Configuration conf) {
    List<Path> roots = new ArrayList<>();
    if (conf == null) {
      return roots;
    }
    Collection<String> configured =
        conf.getTrimmedStringCollection(AngelConf.ANGEL_MODEL_PATH_ALLOWED_ROOTS);
    for (String root : configured) {
      if (root == null || root.trim().isEmpty()) {
        continue;
      }
      try {
        roots.add(new Path(root.trim()));
      } catch (IllegalArgumentException e) {
        LOG.warn("ignore invalid model path root " + root, e);
      }
    }
    return roots;
  }

  private static boolean isUnderAnyRoot(Path target, List<Path> roots) {
    URI targetUri = target.toUri().normalize();
    for (Path root : roots) {
      URI rootUri = root.toUri().normalize();
      if (rootUri.getScheme() != null
          && !Objects.equals(rootUri.getScheme(), targetUri.getScheme())) {
        continue;
      }
      if (rootUri.getAuthority() != null
          && !Objects.equals(rootUri.getAuthority(), targetUri.getAuthority())) {
        continue;
      }
      String rootPath = rootUri.getPath();
      String targetPath = targetUri.getPath();
      if (rootPath == null || targetPath == null) {
        continue;
      }
      if (!rootPath.endsWith("/")) {
        rootPath = rootPath + "/";
      }
      if (targetPath.startsWith(rootPath)
          || targetPath.equals(rootPath.substring(0, rootPath.length() - 1))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Block the silent recursive deletion of a directory that Angel did not create.
   * File targets and genuine Angel model directories keep the original behaviour.
   */
  public static void ensureSafeOverwrite(FileSystem fs, Path outputPath) throws IOException {
    Configuration conf = fs.getConf();
    if (conf == null || !conf.getBoolean(AngelConf.ANGEL_MODEL_SAFE_OVERWRITE,
        AngelConf.DEFAULT_ANGEL_MODEL_SAFE_OVERWRITE)) {
      return;
    }

    FileStatus status;
    try {
      status = fs.getFileStatus(outputPath);
    } catch (FileNotFoundException e) {
      return;
    }
    if (!status.isDirectory() || isAngelModelDirectory(fs, outputPath)) {
      return;
    }
    if (conf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, false)) {
      LOG.warn("overwriting non-model directory {} because {} is enabled", outputPath,
          AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST);
      return;
    }
    throw new IOException("Refuse to recursively delete " + outputPath
        + ": it is not an Angel model directory. Set "
        + AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST
        + "=true only when overwriting an existing directory is intentional.");
  }

  private static boolean isAngelModelDirectory(FileSystem fs, Path dir) throws IOException {
    if (fs.exists(new Path(dir, OWNER_FILE))
        || fs.exists(new Path(dir, ModelFilesConstent.modelMetaFileName))) {
      return true;
    }
    FileStatus[] children;
    try {
      children = fs.listStatus(dir);
    } catch (IOException e) {
      throw new IOException("cannot inspect existing output directory " + dir, e);
    }
    for (FileStatus child : children) {
      if (child.isDirectory()
          && fs.exists(new Path(child.getPath(), ModelFilesConstent.modelMetaFileName))) {
        return true;
      }
    }
    return false;
  }

  /** Write a job ownership marker so a later load cannot silently consume another job's model. */
  public static void writeOwnership(FileSystem fs, Path modelDir, Configuration conf) {
    try {
      try (FSDataOutputStream out = fs.create(new Path(modelDir, OWNER_FILE), true)) {
        out.write((owner(conf) + "\n").getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      LOG.warn("failed to write model owner marker for " + modelDir, e);
    }
  }

  /** Legacy directories without a marker are accepted with a warning for backwards compatibility. */
  public static void checkOwnership(FileSystem fs, Path modelDir, Configuration conf)
      throws IOException {
    if (conf == null || !conf.getBoolean(AngelConf.ANGEL_MODEL_OWNER_CHECK,
        AngelConf.DEFAULT_ANGEL_MODEL_OWNER_CHECK)) {
      return;
    }
    Path marker = new Path(modelDir, OWNER_FILE);
    if (!fs.exists(marker)) {
      LOG.warn("model directory {} has no owner marker; accepting it as a legacy model", modelDir);
      return;
    }
    String stored;
    try (FSDataInputStream in = fs.open(marker)) {
      int length = (int) Math.min(4096, fs.getFileStatus(marker).getLen());
      byte[] buffer = new byte[Math.max(1, length)];
      int read = in.read(buffer);
      stored = read > 0 ? new String(buffer, 0, read, StandardCharsets.UTF_8).trim() : "";
    }
    if (!owner(conf).equals(stored)) {
      throw new IOException("Refuse to load model directory " + modelDir
          + ": it belongs to a different job owner (" + stored + ")");
    }
  }

  private static String owner(Configuration conf) {
    if (conf == null) {
      return System.getProperty("user.name", "");
    }
    return conf.get(AngelConf.USER_NAME, System.getProperty("user.name", ""));
  }
}
