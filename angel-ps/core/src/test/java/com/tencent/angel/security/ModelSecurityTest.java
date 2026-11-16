package com.tencent.angel.security;

import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.model.output.format.BinaryCSRFormat;
import com.tencent.angel.model.output.format.MatrixFormatImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModelSecurityTest {
  private Configuration conf;
  private FileSystem fs;
  private Path root;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.set(AngelConf.USER_NAME, "job-owner");
    root = new Path("file:///tmp/angel-model-security-" + UUID.randomUUID());
    fs = root.getFileSystem(conf);
    fs.mkdirs(root);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(root, true);
  }

  @Test
  public void rejectTraversalAndMalformedPaths() {
    assertInvalid(null, "save");
    assertInvalid("   ", "save");
    assertInvalid("file:///tmp/model/../../victim", "save");
    assertInvalid("file://evil-host/tmp/model", "load");
  }

  @Test
  public void normalizeValidPath() throws IOException {
    Path path = ModelPathGuard.validate(conf, "file:///tmp/angel/./model", "save");
    assertEquals("/tmp/angel/model", path.toUri().getPath());
  }

  @Test
  public void enforceConfiguredAllowedRoots() throws IOException {
    conf.set(AngelConf.ANGEL_MODEL_PATH_ALLOWED_ROOTS, "file:///tmp/allowed");
    ModelPathGuard.validate(conf, "file:///tmp/allowed/model1", "save");
    assertInvalid("file:///tmp/allowed-other/model", "save");
    assertInvalid("file:///tmp/victim", "load");
  }

  @Test
  public void refuseRecursiveDeletionOfNonModelDirectory() throws IOException {
    Path victim = new Path(root, "victim");
    fs.mkdirs(victim);
    fs.create(new Path(victim, "keep.txt"), true).close();
    try {
      ModelPathGuard.ensureSafeOverwrite(fs, victim);
      fail("a non-model directory must not be recursively deleted");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("not an Angel model directory"));
    }
    assertTrue(fs.exists(new Path(victim, "keep.txt")));
  }

  @Test
  public void allowOverwriteOfAngelModelDirectory() throws IOException {
    Path matrix = new Path(new Path(root, "model"), "w1");
    fs.mkdirs(matrix);
    fs.create(new Path(matrix, "_meta"), true).close();
    ModelPathGuard.ensureSafeOverwrite(fs, new Path(root, "model"));
  }

  @Test
  public void allowNewTargetAndFileTarget() throws IOException {
    ModelPathGuard.ensureSafeOverwrite(fs, new Path(root, "not-exist-yet"));
    Path file = new Path(root, "part-00000");
    fs.create(file, true).close();
    ModelPathGuard.ensureSafeOverwrite(fs, file);
  }

  @Test
  public void ownershipMarkerRoundTrip() throws IOException {
    Path model = new Path(root, "owned");
    fs.mkdirs(model);
    ModelPathGuard.writeOwnership(fs, model, conf);
    ModelPathGuard.checkOwnership(fs, model, conf);
    Configuration other = new Configuration(conf);
    other.set(AngelConf.USER_NAME, "intruder");
    try {
      ModelPathGuard.checkOwnership(fs, model, other);
      fail("a model owned by another job must be rejected");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("different job owner"));
    }
  }

  @Test
  public void legacyModelWithoutMarkerIsAccepted() throws IOException {
    Path model = new Path(root, "legacy");
    fs.mkdirs(model);
    ModelPathGuard.checkOwnership(fs, model, conf);
  }

  @Test
  public void restrictModelFormatClasses() throws IOException {
    RpcClassGuard.checkFormatClass(BinaryCSRFormat.class.getName(), conf);
    try {
      RpcClassGuard.checkFormatClass("java.lang.String", conf);
      fail("arbitrary classes must be rejected");
    } catch (IOException expected) {
      // expected
    }
    try {
      RpcClassGuard.checkFormatClass(MatrixFormatImpl.class.getName(), conf);
      fail("abstract format classes must be rejected");
    } catch (IOException expected) {
      // expected
    }
  }

  @Test
  public void restrictRpcProtocolClasses() throws ClassNotFoundException {
    assertEquals(MasterProtocol.class,
        RpcClassGuard.resolveProtocol(MasterProtocol.class.getName(), conf));
    try {
      RpcClassGuard.resolveProtocol("java.lang.String", conf);
      fail("arbitrary protocol classes must be rejected");
    } catch (ClassNotFoundException expected) {
      // expected
    }
    try {
      RpcClassGuard.resolveProtocol("com.tencent.angel.master.MasterService", conf);
      fail("non-interface protocol classes must be rejected");
    } catch (ClassNotFoundException expected) {
      // expected
    }
  }

  @Test
  public void rejectConflictingPSRegistration() {
    Location registered = new Location("10.0.0.1", 9000);
    assertFalse(PsRegistrationGuard.conflicts(registered, new Location("10.0.0.1", 9000)));
    assertTrue(PsRegistrationGuard.conflicts(registered, new Location("127.0.0.1", 6666)));
    assertTrue(PsRegistrationGuard.conflicts(registered, new Location("10.0.0.1", 6666)));
  }

  @Test
  public void strictSourceCheckRejectsSpoofedLoopback() {
    Location advertised = new Location("127.0.0.1", 6666);
    assertTrue(PsRegistrationGuard.sourceMatches(null, advertised, true));
    assertTrue(PsRegistrationGuard.sourceMatches("/127.0.0.1:54321", advertised, true));
    assertFalse(PsRegistrationGuard.sourceMatches("/10.0.0.9:54321", advertised, true));
    assertTrue(PsRegistrationGuard.sourceMatches("/10.0.0.9:54321", advertised, false));
    assertEquals("10.0.0.9", PsRegistrationGuard.extractRemoteIp("/10.0.0.9:54321"));
  }

  private void assertInvalid(String rawPath, String operation) {
    try {
      ModelPathGuard.validate(conf, rawPath, operation);
      fail("expected rejection for path: " + rawPath);
    } catch (IOException expected) {
      // expected
    }
  }
}
