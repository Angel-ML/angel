package com.tencent.angel.psagent.matrix.transport.router;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashAnyKeysAnyValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashAnyKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashIntKeysAnyValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashIntKeysDoubleValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashIntKeysFloatValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashIntKeysIntValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashIntKeysLongValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashIntKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashLongKeysAnyValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashLongKeysDoubleValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashLongKeysFloatValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashLongKeysIntValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashLongKeysLongValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashLongKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashStringKeysAnyValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashStringKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewDoubleValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewFloatValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewIntKeysAnyValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewIntKeysDoubleValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewIntKeysFloatValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewIntKeysIntValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewIntKeysLongValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewIntKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewIntValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewLongKeysAnyValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewLongKeysDoubleValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewLongKeysFloatValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewLongKeysIntValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewLongKeysLongValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewLongKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewLongValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.value.AnyValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.value.DoubleValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.value.FloatValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.value.IntValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.value.LongValuesPart;

public class DataPartFactory {

  public static KeyPart createKeyPart(KeyType keyType, RouterType routerType) {
    if (routerType == RouterType.HASH) {
      switch (keyType) {
        case INT:
          return new HashIntKeysPart();

        case LONG:
          return new HashLongKeysPart();

        case STRING:
          return new HashStringKeysPart();

        case ANY:
          return new HashAnyKeysPart();

        default:
          throw new UnsupportedOperationException("Unsupport key type: " + keyType + " for hash router");
      }
    } else {
      switch (keyType) {
        case INT:
          return new RangeViewIntKeysPart();

        case LONG:
          return new RangeViewLongKeysPart();

        default:
          throw new UnsupportedOperationException("Unsupport key type: " + keyType + " for range router");
      }
    }
  }

  public static ValuePart createValuePart(ValueType valueType) {
    switch (valueType) {
      case INT:
        return new IntValuesPart();

      case LONG:
        return new LongValuesPart();

      case FLOAT:
        return new FloatValuesPart();

      case DOUBLE:
        return new DoubleValuesPart();

      case ANY:
        return new AnyValuesPart();

      default:
        throw new UnsupportedOperationException(
            "Unsupport value type: " + valueType);
    }
  }

  public static KeyValuePart createKeyValuePart(RowType keyValueType, RouterType routerType) {
    if (routerType == RouterType.HASH) {
      switch (keyValueType) {
        case T_DOUBLE_SPARSE:
          return new HashIntKeysDoubleValuesPart();

        case T_FLOAT_SPARSE:
          return new HashIntKeysFloatValuesPart();

        case T_LONG_SPARSE:
          return new HashIntKeysLongValuesPart();

        case T_INT_SPARSE:
          return new HashIntKeysIntValuesPart();

        case T_ANY_INTKEY_SPARSE:
          return new HashIntKeysAnyValuesPart();

        case T_DOUBLE_SPARSE_LONGKEY:
          return new HashLongKeysDoubleValuesPart();

        case T_FLOAT_SPARSE_LONGKEY:
          return new HashLongKeysFloatValuesPart();

        case T_LONG_SPARSE_LONGKEY:
          return new HashLongKeysLongValuesPart();

        case T_INT_SPARSE_LONGKEY:
          return new HashLongKeysIntValuesPart();

        case T_ANY_LONGKEY_SPARSE:
          return new HashLongKeysAnyValuesPart();

        case T_ANY_STRINGKEY_SPARSE:
          return new HashStringKeysAnyValuesPart();

        case T_ANY_ANYKEY_SPARSE:
          return new HashAnyKeysAnyValuesPart();

        default:
          throw new UnsupportedOperationException(
              "Unsupport key type: " + keyValueType + " for hash router");
      }
    } else {
      switch (keyValueType) {
        case T_DOUBLE_DENSE:
          return new RangeViewDoubleValuesPart();

        case T_DOUBLE_SPARSE:
          return new RangeViewIntKeysDoubleValuesPart();

        case T_FLOAT_DENSE:
          return new RangeViewFloatValuesPart();

        case T_FLOAT_SPARSE:
          return new RangeViewIntKeysFloatValuesPart();

        case T_LONG_DENSE:
          return new RangeViewLongValuesPart();

        case T_LONG_SPARSE:
          return new RangeViewIntKeysLongValuesPart();

        case T_INT_DENSE:
          return new RangeViewIntValuesPart();

        case T_INT_SPARSE:
          return new RangeViewIntKeysIntValuesPart();

        case T_ANY_INTKEY_SPARSE:
          return new RangeViewIntKeysAnyValuesPart();

        case T_DOUBLE_SPARSE_LONGKEY:
          return new RangeViewLongKeysDoubleValuesPart();

        case T_FLOAT_SPARSE_LONGKEY:
          return new RangeViewLongKeysFloatValuesPart();

        case T_LONG_SPARSE_LONGKEY:
          return new RangeViewLongKeysLongValuesPart();

        case T_INT_SPARSE_LONGKEY:
          return new RangeViewLongKeysIntValuesPart();

        case T_ANY_LONGKEY_SPARSE:
          return new RangeViewLongKeysAnyValuesPart();

        default:
          throw new UnsupportedOperationException(
              "Unsupport key type: " + keyValueType + " for range router");
      }
    }
  }
}
