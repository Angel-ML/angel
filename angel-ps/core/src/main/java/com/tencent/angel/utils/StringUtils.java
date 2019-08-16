/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.utils;

import org.apache.hadoop.fs.Path;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * General string utils.
 */
public class StringUtils {
  private static final DecimalFormat decimalFormat;
  private static DecimalFormat oneDecimal;
  public static final char COMMA = ',';
  public static final String COMMA_STR = ",";
  public static final char ESCAPE_CHAR = '\\';

  public static String stringifyException(Throwable e) {
    StringWriter stm = new StringWriter();
    PrintWriter wrt = new PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }

  public static String simpleHostname(String fullHostname) {
    int offset = fullHostname.indexOf(46);
    if (offset != -1) {
      return fullHostname.substring(0, offset);
    }
    return fullHostname;
  }

  public static String humanReadableInt(long number) {
    long absNumber = Math.abs(number);
    double result = number;
    String suffix = "";
    if (absNumber >= 1024L) {
      if (absNumber < 1048576L) {
        result = number / 1024.0D;
        suffix = "k";
      } else if (absNumber < 1073741824L) {
        result = number / 1048576.0D;
        suffix = "m";
      } else {
        result = number / 1073741824.0D;
        suffix = "g";
      }
    }
    return new StringBuilder().append(oneDecimal.format(result)).append(suffix).toString();
  }

  public static String formatPercent(double done, int digits) {
    DecimalFormat percentFormat = new DecimalFormat("0.00%");
    double scale = Math.pow(10.0D, digits + 2);
    double rounded = Math.floor(done * scale);
    percentFormat.setDecimalSeparatorAlwaysShown(false);
    percentFormat.setMinimumFractionDigits(digits);
    percentFormat.setMaximumFractionDigits(digits);
    return percentFormat.format(rounded / scale);
  }

  public static String arrayToString(String[] strs) {
    if (strs.length == 0)
      return "";
    StringBuilder sbuf = new StringBuilder();
    sbuf.append(strs[0]);
    for (int idx = 1; idx < strs.length; idx++) {
      sbuf.append(",");
      sbuf.append(strs[idx]);
    }
    return sbuf.toString();
  }

  public static String arrayToString(float[] strs) {
    if (strs.length == 0)
      return "";
    StringBuilder sbuf = new StringBuilder();
    sbuf.append(strs[0]);
    for (int idx = 1; idx < strs.length; idx++) {
      sbuf.append(",");
      sbuf.append(strs[idx]);
    }
    return sbuf.toString();
  }

  public static String byteToHexString(byte[] bytes, int start, int end) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes == null");
    }
    StringBuilder s = new StringBuilder();
    for (int i = start; i < end; i++) {
      s.append(String.format("%02x", new Object[] {bytes[i]}));
    }
    return s.toString();
  }

  public static String byteToHexString(byte[] bytes) {
    return byteToHexString(bytes, 0, bytes.length);
  }

  public static byte[] hexStringToByte(String hex) {
    byte[] bts = new byte[hex.length() / 2];
    for (int i = 0; i < bts.length; i++) {
      bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return bts;
  }

  public static String uriToString(URI[] uris) {
    if (uris == null) {
      return null;
    }
    StringBuilder ret = new StringBuilder(uris[0].toString());
    for (int i = 1; i < uris.length; i++) {
      ret.append(",");
      ret.append(uris[i].toString());
    }
    return ret.toString();
  }

  public static URI[] stringToURI(String[] str) {
    if (str == null)
      return null;
    URI[] uris = new URI[str.length];
    for (int i = 0; i < str.length; i++) {
      try {
        uris[i] = new URI(str[i]);
      } catch (URISyntaxException ur) {
        System.out.println("Exception in specified URI's " + stringifyException(ur));

        uris[i] = null;
      }
    }
    return uris;
  }

  public static Path[] stringToPath(String[] str) {
    if (str == null) {
      return null;
    }
    Path[] p = new Path[str.length];
    for (int i = 0; i < str.length; i++) {
      p[i] = new Path(str[i]);
    }
    return p;
  }

  public static String formatTimeDiff(long finishTime, long startTime) {
    long timeDiff = finishTime - startTime;
    return formatTime(timeDiff);
  }

  public static String formatTime(long timeDiff) {
    StringBuilder buf = new StringBuilder();
    long hours = timeDiff / 3600000L;
    long rem = timeDiff % 3600000L;
    long minutes = rem / 60000L;
    rem %= 60000L;
    long seconds = rem / 1000L;

    if (hours != 0L) {
      buf.append(hours);
      buf.append("hrs, ");
    }
    if (minutes != 0L) {
      buf.append(minutes);
      buf.append("mins, ");
    }

    buf.append(seconds);
    buf.append("sec");
    return buf.toString();
  }

  public static String getFormattedTimeWithDiff(DateFormat dateFormat, long finishTime,
    long startTime) {
    StringBuilder buf = new StringBuilder();
    if (0L != finishTime) {
      buf.append(dateFormat.format(new Date(finishTime)));
      if (0L != startTime) {
        buf.append(" (" + formatTimeDiff(finishTime, startTime) + ")");
      }
    }
    return buf.toString();
  }

  public static String[] getStrings(String str) {
    Collection values = getStringCollection(str);
    if (values.size() == 0) {
      return null;
    }
    return (String[]) values.toArray(new String[values.size()]);
  }

  public static Collection<String> getStringCollection(String str) {
    List values = new ArrayList();
    if (str == null)
      return values;
    StringTokenizer tokenizer = new StringTokenizer(str, ",");
    values = new ArrayList();
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return values;
  }

  public static String[] split(String str) {
    return split(str, '\\', ',');
  }

  public static String[] split(String str, char escapeChar, char separator) {
    if (str == null) {
      return null;
    }
    ArrayList strList = new ArrayList();
    StringBuilder split = new StringBuilder();
    int index = 0;
    while ((index = findNext(str, separator, escapeChar, index, split)) >= 0) {
      index++;
      strList.add(split.toString());
      split.setLength(0);
    }
    strList.add(split.toString());

    int last = strList.size();
    while (true) {
      last--;
      if ((last < 0) || (!"".equals(strList.get(last))))
        break;
      strList.remove(last);
    }
    return (String[]) strList.toArray(new String[strList.size()]);
  }

  public static int findNext(String str, char separator, char escapeChar, int start,
    StringBuilder split) {
    int numPreEscapes = 0;
    for (int i = start; i < str.length(); i++) {
      char curChar = str.charAt(i);
      if ((numPreEscapes == 0) && (curChar == separator)) {
        return i;
      }
      split.append(curChar);
      numPreEscapes++;
      numPreEscapes = curChar == escapeChar ? numPreEscapes % 2 : 0;
    }

    return -1;
  }

  public static String escapeString(String str) {
    return escapeString(str, '\\', ',');
  }

  public static String escapeString(String str, char escapeChar, char charToEscape) {
    return escapeString(str, escapeChar, new char[] {charToEscape});
  }

  private static boolean hasChar(char[] chars, char character) {
    for (char target : chars) {
      if (character == target) {
        return true;
      }
    }
    return false;
  }

  public static String escapeString(String str, char escapeChar, char[] charsToEscape) {
    if (str == null) {
      return null;
    }
    int len = str.length();

    StringBuilder result = new StringBuilder((int) (len * 1.5D));

    for (int i = 0; i < len; i++) {
      char curChar = str.charAt(i);
      if ((curChar == escapeChar) || (hasChar(charsToEscape, curChar))) {
        result.append(escapeChar);
      }
      result.append(curChar);
    }
    return result.toString();
  }

  public static String unEscapeString(String str) {
    return unEscapeString(str, '\\', ',');
  }

  public static String unEscapeString(String str, char escapeChar, char charToEscape) {
    return unEscapeString(str, escapeChar, new char[] {charToEscape});
  }

  public static String unEscapeString(String str, char escapeChar, char[] charsToEscape) {
    if (str == null) {
      return null;
    }
    StringBuilder result = new StringBuilder(str.length());
    boolean hasPreEscape = false;
    for (int i = 0; i < str.length(); i++) {
      char curChar = str.charAt(i);
      if (hasPreEscape) {
        if ((curChar != escapeChar) && (!hasChar(charsToEscape, curChar))) {
          throw new IllegalArgumentException(
            "Illegal escaped string " + str + " unescaped " + escapeChar + " at " + (i - 1));
        }

        result.append(curChar);
        hasPreEscape = false;
      } else {
        if (hasChar(charsToEscape, curChar)) {
          throw new IllegalArgumentException(
            "Illegal escaped string " + str + " unescaped " + curChar + " at " + i);
        }
        if (curChar == escapeChar)
          hasPreEscape = true;
        else {
          result.append(curChar);
        }
      }
    }
    if (hasPreEscape) {
      throw new IllegalArgumentException(
        "Illegal escaped string " + str + ", not expecting " + escapeChar + " in the end.");
    }

    return result.toString();
  }

  // public static String getHostname() {
  // try {
  // return new StringBuilder().append("").append(InetAddress.getLocalHost()).toString();
  // } catch (UnknownHostException uhe) {
  // }
  // return new StringBuilder().append("").append(uhe).toString();
  // }

  private static String toStartupShutdownString(String prefix, String[] msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for (String s : msg)
      b.append("\n" + prefix + s);
    b.append("\n************************************************************/");
    return b.toString();
  }

  // public static void startupShutdownMessage(Class<?> clazz, String[] args, Log LOG) {
  // String hostname = getHostname();
  // String classname = clazz.getSimpleName();
  // LOG.info(toStartupShutdownString("STARTUP_MSG: ", new String[]{new
  // StringBuilder().append("Starting ").append(classname).toString(), new
  // StringBuilder().append("  host = ").append(hostname).toString(), new
  // StringBuilder().append("  args = ").append(Arrays.asList(args)).toString(), new
  // StringBuilder().append("  version = ").append(VersionInfo.getVersion()).toString(), new
  // StringBuilder().append("  build = ").append(VersionInfo.getUrl()).append(" -r ").append(VersionInfo.getRevision()).append("; compiled by '").append(VersionInfo.getUser()).append("' on ").append(VersionInfo.getDate()).toString()}));
  //
  // Runtime.getRuntime().addShutdownHook(new Thread(LOG, classname, hostname) {
  // public void run() {
  // this.val$LOG.info(StringUtils.access$000("SHUTDOWN_MSG: ", new String[]{"Shutting down " +
  // this.val$classname + " at " + this.val$hostname}));
  // }
  // });
  // }

  public static String escapeHTML(String string) {
    if (string == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    boolean lastCharacterWasSpace = false;
    char[] chars = string.toCharArray();
    for (char c : chars) {
      if (c == ' ') {
        if (lastCharacterWasSpace) {
          lastCharacterWasSpace = false;
          sb.append("&nbsp;");
        } else {
          lastCharacterWasSpace = true;
          sb.append(" ");
        }
      } else {
        lastCharacterWasSpace = false;
        switch (c) {
          case '<':
            sb.append("&lt;");
            break;
          case '>':
            sb.append("&gt;");
            break;
          case '&':
            sb.append("&amp;");
            break;
          case '"':
            sb.append("&quot;");
            break;
          default:
            sb.append(c);
        }
      }
    }

    return sb.toString();
  }

  public static String byteDesc(long len) {
    double val = 0.0D;
    String ending = "";
    if (len < 1048576L) {
      val = 1.0D * len / 1024.0D;
      ending = " KB";
    } else if (len < 1073741824L) {
      val = 1.0D * len / 1048576.0D;
      ending = " MB";
    } else if (len < 1099511627776L) {
      val = 1.0D * len / 1073741824.0D;
      ending = " GB";
    } else if (len < 1125899906842624L) {
      val = 1.0D * len / 1099511627776.0D;
      ending = " TB";
    } else {
      val = 1.0D * len / 1125899906842624.0D;
      ending = " PB";
    }
    return limitDecimalTo2(val) + ending;
  }

  public static synchronized String limitDecimalTo2(double d) {
    return decimalFormat.format(d);
  }

  public static String join(CharSequence separator, Iterable<String> strings) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : strings) {
      if (first)
        first = false;
      else {
        sb.append(separator);
      }
      sb.append(s);
    }
    return sb.toString();
  }

  public static String join(CharSequence separator, String[] strings) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : strings) {
      if (first)
        first = false;
      else {
        sb.append(separator);
      }
      sb.append(s);
    }
    return sb.toString();
  }

  public static String capitalize(String s) {
    int len = s.length();
    if (len == 0)
      return s;
    return String.valueOf(Character.toTitleCase(s.charAt(0))) + s.substring(1);
  }

  public static String camelize(String s) {
    StringBuilder sb = new StringBuilder();
    String[] words = split(s.toLowerCase(Locale.US), '\\', '_');

    for (String word : words) {
      sb.append(capitalize(word));
    }
    return sb.toString();
  }

  static {
    NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ENGLISH);
    decimalFormat = (DecimalFormat) numberFormat;
    decimalFormat.applyPattern("#.##");

    oneDecimal = new DecimalFormat("0.0");
  }


  public static enum TraditionalBinaryPrefix {
    KILO(1024L), MEGA(KILO.value << 10), GIGA(MEGA.value << 10), TERA(GIGA.value << 10), PETA(
      TERA.value << 10), EXA(PETA.value << 10);

    public final long value;
    public final char symbol;

    private TraditionalBinaryPrefix(long value) {
      this.value = value;
      this.symbol = toString().charAt(0);
    }

    public static TraditionalBinaryPrefix valueOf(char symbol) {
      symbol = Character.toUpperCase(symbol);
      for (TraditionalBinaryPrefix prefix : values()) {
        if (symbol == prefix.symbol) {
          return prefix;
        }
      }
      throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
    }

    public static long string2long(String s) {
      s = s.trim();
      int lastpos = s.length() - 1;
      char lastchar = s.charAt(lastpos);
      if (Character.isDigit(lastchar)) {
        return Long.parseLong(s);
      }
      long prefix = valueOf(lastchar).value;
      long num = Long.parseLong(s.substring(0, lastpos));
      if ((num > 9223372036854775807L / prefix) || (num < -9223372036854775808L / prefix)) {
        throw new IllegalArgumentException(s + " does not fit in a Long");
      }
      return num * prefix;
    }
  }

  public static String joinInts(CharSequence separator, Iterable<Integer> ints) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int s : ints) {
      if (first)
        first = false;
      else {
        sb.append(separator);
      }
      sb.append(s);
    }
    return sb.toString();
  }

  public static String join(CharSequence separator, Map<String, String> kvMap) {
    if (kvMap == null || kvMap.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, String> entry : kvMap.entrySet()) {
      if (first)
        first = false;
      else {
        sb.append(separator);
      }
      sb.append(entry.getKey() + ":" + entry.getValue());
    }
    return sb.toString();
  }
}
