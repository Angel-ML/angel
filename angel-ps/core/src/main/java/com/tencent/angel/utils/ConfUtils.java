package com.tencent.angel.utils;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.InvalidParameterException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfUtils {
  private static final Log LOG = LogFactory.getLog(AngelRunJar.class);
  private static final String angelSysConfFile = "angel-site.xml";

  public static Configuration initConf(String [] cmdArgs) throws Exception {
    // Parse cmd parameters
    Map<String, String> cmdConfMap = parseArgs(cmdArgs);

    // load hadoop configuration
    final Configuration conf = new Configuration();
    String hadoopHomePath = System.getenv("HADOOP_HOME");
    if (hadoopHomePath == null) {
      LOG.warn("HADOOP_HOME is empty.");
    } else {
      conf.addResource(new Path(hadoopHomePath + "/etc/hadoop/yarn-site.xml"));
      conf.addResource(new Path(hadoopHomePath + "/etc/hadoop/hdfs-site.xml"));
    }

    // load angel system configuration
    String angelHomePath = System.getenv("ANGEL_HOME");
    if (angelHomePath == null) {
      LOG.fatal("ANGEL_HOME is empty, please set it first");
      throw new InvalidParameterException("ANGEL_HOME is empty, please set it first");
    }
    LOG.info("angelHomePath conf path=" + angelHomePath + "/conf/" + angelSysConfFile);
    conf.addResource(new Path(angelHomePath + "/conf/" + angelSysConfFile));
    LOG.info("load system config file success");

    // load user configuration:
    // load user config file
    String jobConfFile = cmdConfMap.get(AngelConf.ANGEL_APP_CONFIG_FILE);
    if(jobConfFile != null) {
      LOG.info("user app config file " + jobConfFile);
      conf.addResource(new Path(jobConfFile));
    } else {
      jobConfFile = conf.get(AngelConf.ANGEL_APP_CONFIG_FILE);
      if(jobConfFile != null) {
        LOG.info("user app config file " + jobConfFile);
        conf.addResource(new Path(jobConfFile));
      }
    }

    // load command line parameters
    if(cmdConfMap != null && !cmdConfMap.isEmpty()) {
      for(Map.Entry<String, String> entry : cmdConfMap.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }

    // load user job resource files
    String userResourceFiles = conf.get(AngelConf.ANGEL_APP_USER_RESOURCE_FILES);
    if(userResourceFiles != null) {
      addResourceFiles(conf, userResourceFiles);
    }

    // load user job jar if it exist
    String jobJar = conf.get(AngelConf.ANGEL_JOB_JAR);
    if (jobJar != null) {
      loadJar(jobJar);
      addResourceFiles(conf, jobJar);
    }

    // Expand the environment variable
    try {
      expandEnv(conf);
    } catch (Exception x) {
      LOG.warn("expand env in configuration failed.", x);
    }
    return conf;
  }

  private static Map<String, String> parseArgs(String[] args) throws InvalidParameterException {
    Map<String, String> kvMap = new HashMap<String, String>();

    int seg = 0;
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("-D")) {
        seg = args[i].indexOf("=");
        if (seg > 0) {
          kvMap.put(args[i].substring(2, seg), args[i].substring(seg + 1));
        } else {
          throw new InvalidParameterException("unvalid parameter " + args[i]);
        }
      } else if (args[i].startsWith("--")) {
        String key = args[i].substring(2);
        i++;
        if (i < args.length) {
          String value = args[i];
          kvMap.put(key, value);
        } else {
          throw new InvalidParameterException("there is no value for parameter " + key);
        }
      } else if ((seg = args[i].indexOf(":")) > 0) {
        kvMap.put(args[i].substring(0, seg), args[i].substring(seg + 1));
      } else {
        switch (args[i]) {
          case "jar": {
            if (i == args.length - 1) {
              throw new InvalidParameterException("there must be a jar file after jar commond");
            } else {
              i++;
              kvMap.put(AngelConf.ANGEL_JOB_JAR, args[i]);
            }
            break;
          }
          default: {
            throw new InvalidParameterException("unvalid parameter " + args[i]);
          }
        }
      }
    }
    return kvMap;
  }

  private static void expandEnv(Configuration conf) {

    Map<String, String> kvs = conf.getValByRegex("angel.*");
    Pattern pattern = Pattern.compile("\\$\\{[\\p{Alnum}\\p{Punct}]+?\\}");

    for (Map.Entry<String, String> kv : kvs.entrySet()) {
      String value = kv.getValue();
      Matcher matcher = pattern.matcher(value);
      List<String> keys = new ArrayList<String>();

      while (matcher.find()) {
        String matchedStr = matcher.group();
        keys.add(matchedStr.substring(2, matchedStr.length() - 1));
      }

      int size = keys.size();
      for (int i = 0; i < size; i++) {
        String envValue = System.getenv(keys.get(i));
        if (envValue == null) {
          LOG.warn("env " + keys.get(i) + " is null, please check.");
          continue;
        }
        value = value.replaceAll("\\$\\{" + keys.get(i) + "\\}", envValue);
      }

      conf.set(kv.getKey(), value);
    }

    // Add default fs(local fs) for lib jars.
    String libJars = conf.get(AngelConf.ANGEL_JOB_LIBJARS);
    if (libJars != null) {
      StringBuilder sb = new StringBuilder();
      String[] jars = libJars.split(",");
      for (int i = 0; i < jars.length; i++) {
        if (new Path(jars[i]).isAbsoluteAndSchemeAuthorityNull()) {
          sb.append("file://").append(jars[i]);
          if (i != jars.length - 1) {
            sb.append(",");
          }
        } else {
          sb.append(jars[i]);
          if (i != jars.length - 1) {
            sb.append(",");
          }
        }
      }
      conf.set(AngelConf.ANGEL_JOB_LIBJARS, sb.toString());
      LOG.info("jars loaded: " + sb.toString());
    }
  }

  private static void addResourceFiles(Configuration conf, String fileNames)
    throws MalformedURLException {
    String[] fileNameArray = fileNames.split(",");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fileNameArray.length; i++) {
      if (i != 0) {
        sb.append(",");
      }
      URL url = new File(fileNameArray[i]).toURI().toURL();
      sb.append(url.toString());
    }

    String addJars = conf.get(AngelConf.ANGEL_JOB_LIBJARS);

    if (addJars == null || addJars.trim().isEmpty()) {
      conf.set(AngelConf.ANGEL_JOB_LIBJARS, sb.toString());
    } else {
      conf.set(AngelConf.ANGEL_JOB_LIBJARS, sb.toString() + "," + addJars);
    }
  }

  private static void loadJar(String jarFile) throws IOException {
    URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    Class<? extends URLClassLoader> sysclass = URLClassLoader.class;
    try {
      Method method = sysclass.getDeclaredMethod("addURL", URL.class);
      method.setAccessible(true);
      method.invoke(sysloader, new File(jarFile).toURI().toURL());
    } catch (Throwable t) {
      throw new IOException("Error, could not add URL to system classloader", t);
    }
  }
}
