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

/**
 * Murmur hash utils.
 */
public class MurmurHash3 {

  /** Returns the MurmurHash3_x86_32 hash. */
  public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {

    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;

    int h1 = seed;
    int roundedEnd = offset + (len & 0xfffffffc); // round down to 4 byte block

    for (int i = offset; i < roundedEnd; i += 4) {
      // little endian load order
      int k1 =
          (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16)
              | (data[i + 3] << 24);
      k1 *= c1;
      k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
      k1 *= c2;

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> 19); // ROTL32(h1,13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    // tail
    int k1 = 0;

    switch (len & 0x03) {
      case 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16;
        // fallthrough
      case 2:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
        // fallthrough
      case 1:
        k1 |= (data[roundedEnd] & 0xff);
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
        k1 *= c2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }

  private static int unsignedByte(byte b) {
    return ((int) b) & 0xFF;
  }

  public static int murmurhash3_x86_32(byte[] data, int len, int seed) {
    final int nblocks = len / 4;

    int h1 = seed;

    int c1 = 0xcc9e2d51;
    int c2 = 0x1b873593;

    // ----------
    // body

    for (int i = 0; i < nblocks; i++) {
      int k1 = getBlock32(data, 4 * i);

      k1 *= c1;
      k1 = (k1 << 15) | (k1 >>> (32 - 15));
      k1 *= c2;

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> (32 - 13));
      h1 = h1 * 5 + 0xe6546b64;
    }

    // ----------
    // tail

    int tail = 4 * nblocks;

    int k1 = 0;

    switch (len & 3) {
      case 3:
        k1 ^= unsignedByte(data[tail + 2]) << 16;
      case 2:
        k1 ^= unsignedByte(data[tail + 1]) << 8;
      case 1:
        k1 ^= unsignedByte(data[tail]);
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >>> (32 - 15));
        k1 *= c2;
        h1 ^= k1;
    }

    // ----------
    // finalization

    h1 ^= len;

    int h = h1;
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    h1 = h;

    return h1;
  }

  private static int getBlock32(byte[] data, int i) {
    return unsignedByte(data[i]) | unsignedByte(data[i + 1]) << 8 | unsignedByte(data[i + 2]) << 16
        | unsignedByte(data[i + 3]) << 24;
  }

  private static long getBlock64(byte[] data, int i) {
    long low = ((long) getBlock32(data, 8 * i)) & 0xFFFFFFFFL;
    long high = ((long) getBlock32(data, 8 * i + 4)) & 0xFFFFFFFFL;
    return low | high << 32;
  }

  private static long fmix64(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;

    return k;
  }

  public static long murmurhash3_x64_64(byte[] data, int len, int seed) {
    final int nblocks = len / 16;

    long h1 = ((long) seed) & 0xFFFFFFFFL;
    long h2 = h1;

    final long c1 = 0x87c37b91114253d5L;
    final long c2 = 0x4cf5ad432745937fL;

    // ----------
    // body

    for (int i = 0; i < nblocks; i++) {
      long k1 = getBlock64(data, 2 * i);
      long k2 = getBlock64(data, 2 * i + 1);

      k1 *= c1;
      k1 = ROTL64(k1, 31);
      k1 *= c2;
      h1 ^= k1;

      h1 = ROTL64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729L;

      k2 *= c2;
      k2 = ROTL64(k2, 33);
      k2 *= c1;
      h2 ^= k2;

      h2 = ROTL64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5L;
    }

    // ----------
    // tail

    int tail = 16 * nblocks;

    long k1 = 0;
    long k2 = 0;

    switch (len & 15) {
      case 15:
        k2 ^= (long) (unsignedByte(data[tail + 14])) << 48;
      case 14:
        k2 ^= (long) (unsignedByte(data[tail + 13])) << 40;
      case 13:
        k2 ^= (long) (unsignedByte(data[tail + 12])) << 32;
      case 12:
        k2 ^= (long) (unsignedByte(data[tail + 11])) << 24;
      case 11:
        k2 ^= (long) (unsignedByte(data[tail + 10])) << 16;
      case 10:
        k2 ^= (long) (unsignedByte(data[tail + 9])) << 8;
      case 9:
        k2 ^= (long) (unsignedByte(data[tail + 8])) << 0;
        k2 *= c2;
        k2 = ROTL64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

      case 8:
        k1 ^= (long) (unsignedByte(data[tail + 7])) << 56;
      case 7:
        k1 ^= (long) (unsignedByte(data[tail + 6])) << 48;
      case 6:
        k1 ^= (long) (unsignedByte(data[tail + 5])) << 40;
      case 5:
        k1 ^= (long) (unsignedByte(data[tail + 4])) << 32;
      case 4:
        k1 ^= (long) (unsignedByte(data[tail + 3])) << 24;
      case 3:
        k1 ^= (long) (unsignedByte(data[tail + 2])) << 16;
      case 2:
        k1 ^= (long) (unsignedByte(data[tail + 1])) << 8;
      case 1:
        k1 ^= (long) (unsignedByte(data[tail + 0])) << 0;
        k1 *= c1;
        k1 = ROTL64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }

    // ----------
    // finalization

    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    return h1;
  }

  private static long ROTL64(long x, int r) {
    return (x << r) | (x >>> (64 - r));
  }

  public static int murmurhash3_x64_32(final byte[] key, int len, final int seed) {
    return (int) (murmurhash3_x64_64(key, len, seed) >>> 32);
  }
}
