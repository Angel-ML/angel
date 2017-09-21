package com.tencent.angel.example.quickStart;

//import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;

/**
 * Created by payniexiao on 2017/8/21.
 */
public class TestString {
  public static void main(String [] args) {
    String [] paras = {};
    //paras[1] = "abc";
    //paras[2] = "123";
    for(String n:paras) {
      System.out.println(n);
    }

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, ",,1,2,3,4");
    // Add standard Hadoop classes
    for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      System.out.println(c);;
    }
  }
}
