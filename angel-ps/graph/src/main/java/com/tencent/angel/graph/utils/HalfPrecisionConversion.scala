package com.tencent.angel.graph.utils

object HalfPrecisionConversion {

  /**
    * Returns the full-precision float number from the half-precision value
    *
    * @param halfPrecisionValue half-precision value
    * @return the full-precision floating pointing number.
    */
  def fromHalfPrecision(halfPrecisionValue: Short): Float = {
    var mantissa = halfPrecisionValue & 0x03ff
    var exponent = halfPrecisionValue & 0x7c00
    if (exponent == 0x7c00) {
      exponent = 0x3fc00
    } else if (exponent != 0) {
      exponent += 0x1c000
      if (mantissa == 0 && exponent > 0x1c400) {
        return java.lang.Float.intBitsToFloat((halfPrecisionValue & 0x8000) << 16 | exponent << 13)
      }
    }
    else if (mantissa != 0) {
      exponent = 0x1c400
      do {
        mantissa <<= 1
        exponent -= 0x400
      } while ((mantissa & 0x400) == 0)
      mantissa &= 0x3ff
    }
    java.lang.Float.intBitsToFloat((halfPrecisionValue & 0x8000) << 16 | (exponent | mantissa) << 13)
  }

  /**
    * Returns the Int representation of the supplied
    * full-precision floating pointing number.
    *
    * @param fullPrecisionValue the full-precision floating pointing number.
    * @return the Int representation.
    */
  def convertHalfPrecision(fullPrecisionValue: Float): Int = {
    val fbits = java.lang.Float.floatToIntBits(fullPrecisionValue)
    val sign = fbits >>> 16 & 0x8000
    var value = (fbits & 0x7fffffff) + 0x1000
    if (value >= 0x47800000) {
      if ((fbits & 0x7fffffff) >= 0x47800000) {
        if (value < 0x7f800000) {
          return sign | 0x7c00
        }
        return sign | 0x7c00 | (fbits & 0x007fffff) >>> 13
      }
      return sign | 0x7bff
    }
    if (value >= 0x38800000) {
      return sign | value - 0x38000000 >>> 13
    }
    if (value < 0x33000000) {
      return sign
    }
    value = (fbits & 0x7fffffff) >>> 23
    sign | ((fbits & 0x7fffff | 0x800000) + (0x800000 >>> value - 102) >>> 126 - value)
  }

  /**
    * Returns the Short representation of the supplied
    * full-precision floating pointing number.
    *
    * @param fullPrecisionValue the full-precision floating pointing number.
    * @return the Short representation.
    */
  def toHalfPrecision(fullPrecisionValue: Float): Short = {
    if (fullPrecisionValue > Short.MaxValue) {
      throw new IllegalArgumentException("The supplied float is too "
        + "large for a two byte representation")
    }
    if (fullPrecisionValue < Short.MinValue) {
      throw new IllegalArgumentException("The supplied float is too "
        + "small for a two byte representation")
    }
    val value = convertHalfPrecision(fullPrecisionValue)
    value.asInstanceOf[Short]
  }
}
