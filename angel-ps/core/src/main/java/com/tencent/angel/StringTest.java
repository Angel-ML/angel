package com.tencent.angel;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class StringTest {
  public static void main(String[] args) throws IOException {
    String filePath = "C:\\source\\angel-hash\\angel\\data\\bc\\edge";
    String outPath = "C:\\source\\angel-hash\\angel\\data\\bc\\edge_weight";
    BufferedReader reader = new BufferedReader(new FileReader(filePath));
    BufferedWriter writer = new BufferedWriter(new FileWriter(outPath));
    Random r = new Random();
    while(true) {
      String line = reader.readLine();
      if(line != null) {
        writer.write(line + " " + r.nextInt(10) + "\n");
      } else {
        break;
      }
    }

    reader.close();
    writer.close();


    int size = 10000000;
    Object2IntOpenHashMap table1 = new Object2IntOpenHashMap<String>(size);
    Object2IntOpenHashMap table3 = new Object2IntOpenHashMap<ByteArray>(size);
    Int2IntOpenHashMap table2 = new Int2IntOpenHashMap(size);
    String[] keys = new String[size];
    byte[][] bKeys = new byte[size][];
    for(int i = 0; i < size; i++) {
      keys[i] = "" + i;
      bKeys[i] = keys[i].getBytes();
      table1.put(keys[i], i);
      table3.put(bKeys[i], i);
      table2.put(i, i);
    }


    int testTime = 10;
    long sum = 0;
    long startTs = System.currentTimeMillis();
    for(int testIndex = 0; testIndex < testTime; testIndex++) {
      for(int i = 0; i < size; i++) {
        sum += table1.getInt(keys[i]);
      }
    }

    System.out.println("====string test time = " + (System.currentTimeMillis() - startTs));

    startTs = System.currentTimeMillis();
    for(int testIndex = 0; testIndex < testTime; testIndex++) {
      for(int i = 0; i < size; i++) {
        sum += table2.get(i);
      }
    }

    System.out.println("====int test time = " + (System.currentTimeMillis() - startTs));

    startTs = System.currentTimeMillis();
    for(int testIndex = 0; testIndex < testTime; testIndex++) {
      for(int i = 0; i < size; i++) {
        sum += table3.getInt(bKeys[i]);
      }
    }

    System.out.println("====binary test time = " + (System.currentTimeMillis() - startTs));
  }
}
