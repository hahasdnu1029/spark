package org.apache.spark.sql.catalyst.vector;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;

public class OnColumnVector extends ColumnVector {

  public boolean[] isNull;

  public int[] intVector;
  public long[] longVector;
  public double[] doubleVector;

  public byte[][] bytesVector;
  public int[] starts;
  public int[] lengths;

  public UnsafeRow[] rowVector;

  public OnColumnVector(DataType type, int capacity) {
    super(type, capacity);
    isNull = new boolean[capacity];
    if (type instanceof IntegerType) {
      intVector = new int[capacity];
    } else if (type instanceof LongType) {
      longVector = new long[capacity];
    } else if (type instanceof DoubleType) {
      doubleVector = new double[capacity];
    } else if (type instanceof StringType) {
      bytesVector = new byte[capacity][];
      starts = new int[capacity];
      lengths = new int[capacity];
    } else if (type instanceof StructType) {
      rowVector = new UnsafeRow[capacity];
    } else {
      throw new UnsupportedOperationException(type + "is Not supported yet");
    }
  }

  public OnColumnVector(int capacity, DataType type) {
    super(type, capacity);
    isNull = new boolean[capacity];
  }

  // Ctor only used by substring expression
  public OnColumnVector(ColumnVector otherCV) {
    super(otherCV.dataType, otherCV.capacity);
    OnColumnVector other = (OnColumnVector) otherCV;
    isNull = other.isNull;
    noNulls = other.noNulls;
    isRepeating = other.isRepeating;
    bytesVector = other.bytesVector;
    starts = new int[other.starts.length];
    lengths = new int[other.lengths.length];
    System.arraycopy(other.starts, 0, starts, 0, other.starts.length);
    System.arraycopy(other.lengths, 0, lengths, 0, other.lengths.length);
  }

  public static OnColumnVector genIntCV(int capacity) {
    OnColumnVector cv = new OnColumnVector(capacity, IntegerType$.MODULE$);
    cv.intVector = new int[capacity];
    return cv;
  }

  public static OnColumnVector genLongCV(int capacity) {
    OnColumnVector cv = new OnColumnVector(capacity, LongType$.MODULE$);
    cv.longVector = new long[capacity];
    return cv;
  }

  public static OnColumnVector genDoubleCV(int capacity) {
    OnColumnVector cv = new OnColumnVector(capacity, DoubleType$.MODULE$);
    cv.doubleVector = new double[capacity];
    return cv;
  }

  public static OnColumnVector genStringCV(int capacity) {
    OnColumnVector cv = new OnColumnVector(capacity, StringType$.MODULE$);
    cv.bytesVector = new byte[capacity][];
    cv.starts = new int[capacity];
    cv.lengths = new int[capacity];
    return cv;
  }

  public static OnColumnVector genUnsafeRowColumnVector(int capacity, DataType dt) {
    OnColumnVector cv = new OnColumnVector(capacity, dt);
    cv.rowVector = null;
    return cv;
  }

  @Override
  public void putNull(int rowId) {
    isNull[rowId] = true;
  }

  @Override
  public void putNotNull(int rowId) {
    isNull[rowId] = false;
  }

  @Override
  public void setNull(int rowId, boolean value) {
    isNull[rowId] = value;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return isNull[rowId];
  }

  @Override
  public int[] getIntVector() {
    return intVector;
  }

  @Override
  public long[] getLongVector() {
    return longVector;
  }

  @Override
  public double[] getDoubleVector() {
    return doubleVector;
  }

  @Override
  public byte[][] getBytesVector() {
    return bytesVector;
  }

  @Override
  public int[] getStartsVector() {
    return starts;
  }

  @Override
  public int[] getLengthsVector() {
    return lengths;
  }

  @Override
  public UnsafeRow[] getRowVector() {
    return rowVector;
  }

  @Override
  public long getDataNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }

  @Override
  public void putInt(int rowId, int value) {
    intVector[rowId] = value;
  }

  @Override
  public void putLong(int rowId, long value) {
    longVector[rowId] = value;
  }

  @Override
  public void putDouble(int rowId, double value) {
    doubleVector[rowId] = value;
  }

  @Override
  public void putString(int rowId, UTF8String value) {
    bytesVector[rowId] = value.getBytes().clone();
    starts[rowId] = 0;
    lengths[rowId] = value.numBytes();
  }

  @Override
  public void putString(int rowId, String value) {
    byte[] bytes = value.getBytes();
    bytesVector[rowId] = bytes;
    starts[rowId] = 0;
    lengths[rowId] = bytes.length;
  }

  @Override
  public void putByteArray(int rowId, byte[] value) {
    bytesVector[rowId] = value.clone();
    starts[rowId] = 0;
    lengths[rowId] = value.length;
  }

  @Override
  public void putByteArray(int rowId, byte[] value, int offset, int length) {
    byte[] bytes = new byte[length];
    System.arraycopy(value, offset, bytes, 0, length);
    bytesVector[rowId] = bytes;
    starts[rowId] = 0;
    lengths[rowId] = length;
  }


  @Override
  public void putNulls(int destPos, int length) {
    if (length > 0) {
      noNulls = false;
    }
    for (int i = 0; i < length; i ++) {
      isNull[destPos + i] = true;
    }
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    for (int i = 0; i < count; ++i) {
      intVector[i + rowId] = value;
    }
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    for (int i = 0; i < count; ++i) {
      longVector[i + rowId] = value;
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    Arrays.fill(doubleVector, rowId, rowId + count, value);
  }

  @Override
  public void putStrings(int rowId, int count, String value) {
    throw new RuntimeException("not supported yet");
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, intVector, rowId, count);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i) {
      intVector[i + rowId] = Platform.getInt(src, srcOffset);
      srcIndex += 4;
      srcOffset += 4;
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, longVector, rowId, count);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i) {
      longVector[i + rowId] = Platform.getLong(src, srcOffset);
      srcIndex += 8;
      srcOffset += 8;
    }
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, doubleVector,
      Platform.DOUBLE_ARRAY_OFFSET + rowId * 8, count * 8);
  }

  @Override
  public void putStrings(int rowId, int count, UTF8String[] src, int srcIndex) {
    throw new RuntimeException("not supported yet");
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Ints set
  //////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void putIntsRepeat(ColumnVector src, int srcPos, int destPos, int repeat) {
    OnColumnVector on = (OnColumnVector) src;
    int value = on.intVector[srcPos];
    for (int i = 0; i < repeat; i ++) {
      intVector[destPos + i] = value;
    }
  }

  @Override
  public void putIntsRepeats(ColumnVector src, int srcPos, int destPos, int repeat, int length) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < length; i ++) {
      int value = on.intVector[srcPos + i];
      for (int j = 0; j < repeat; j ++, destIdx ++) {
        intVector[destIdx] = value;
      }
    }
  }

  @Override
  public void putIntsRun(ColumnVector src, int srcPos, int destPos, int length) {
    OnColumnVector on = (OnColumnVector) src;
    for (int i = 0; i < length; i ++) {
      intVector[destPos + i] = on.intVector[srcPos + i];
    }
  }

  @Override
  public void putIntsRuns(ColumnVector src, int srcPos, int destPos, int repeat, int length) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < repeat; i ++) {
      int srcIdx = srcPos;
      for (int j = 0; j < length; j ++, srcIdx ++, destIdx ++) {
        intVector[destIdx] = on.intVector[srcIdx];
      }
    }
  }

  @Override
  public void putIntsRunsWithStep(ColumnVector src, int srcPos, int destPos, int repeat, int length, int step) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < repeat; i ++) {
      int srcIdx = srcPos;
      for (int j = 0; j < length; j ++, srcIdx ++, destIdx ++) {
        intVector[destIdx] = on.intVector[srcIdx];
      }
      destIdx += step;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Longs set
  //////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void putLongsRepeat(ColumnVector src, int srcPos, int destPos, int repeat) {
    OnColumnVector on = (OnColumnVector) src;
    long value = on.longVector[srcPos];
    for (int i = 0; i < repeat; i ++) {
      longVector[destPos + i] = value;
    }
  }

  @Override
  public void putLongsRepeats(ColumnVector src, int srcPos, int destPos, int repeat, int length) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < length; i ++) {
      long value = on.longVector[srcPos + i];
      for (int j = 0; j < repeat; j ++, destIdx ++) {
        longVector[destIdx] = value;
      }
    }
  }

  @Override
  public void putLongsRun(ColumnVector src, int srcPos, int destPos, int length) {
    OnColumnVector on = (OnColumnVector) src;
    for (int i = 0; i < length; i ++) {
      longVector[destPos + i] = on.longVector[srcPos + i];
    }
  }

  @Override
  public void putLongsRuns(ColumnVector src, int srcPos, int destPos, int repeat, int length) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < repeat; i ++) {
      int srcIdx = srcPos;
      for (int j = 0; j < length; j ++, srcIdx ++, destIdx ++) {
        longVector[destIdx] = on.longVector[srcIdx];
      }
    }
  }

  @Override
  public void putLongsRunsWithStep(ColumnVector src, int srcPos, int destPos, int repeat, int length, int step) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < repeat; i ++) {
      int srcIdx = srcPos;
      for (int j = 0; j < length; j ++, srcIdx ++, destIdx ++) {
        longVector[destIdx] = on.longVector[srcIdx];
      }
      destIdx += step;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Doubles set
  //////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void putDoublesRepeat(ColumnVector src, int srcPos, int destPos, int repeat) {
    OnColumnVector on = (OnColumnVector) src;
    double value = on.doubleVector[srcPos];
    for (int i = 0; i < repeat; i ++) {
      doubleVector[destPos + i] = value;
    }
  }

  @Override
  public void putDoublesRepeats(ColumnVector src, int srcPos, int destPos, int repeat, int length) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < length; i ++) {
      double value = on.doubleVector[srcPos + i];
      for (int j = 0; j < repeat; j ++, destIdx ++) {
        doubleVector[destIdx] = value;
      }
    }
  }

  @Override
  public void putDoublesRun(ColumnVector src, int srcPos, int destPos, int length) {
    OnColumnVector on = (OnColumnVector) src;
    for (int i = 0; i < length; i ++) {
      doubleVector[destPos + i] = on.doubleVector[srcPos + i];
    }
  }

  @Override
  public void putDoublesRuns(ColumnVector src, int srcPos, int destPos, int repeat, int length) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < repeat; i ++) {
      int srcIdx = srcPos;
      for (int j = 0; j < length; j ++, srcIdx ++, destIdx ++) {
        doubleVector[destIdx] = on.doubleVector[srcIdx];
      }
    }
  }

  @Override
  public void putDoublesRunsWithStep(ColumnVector src, int srcPos, int destPos, int repeat, int length, int step) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < repeat; i ++) {
      int srcIdx = srcPos;
      for (int j = 0; j < length; j ++, srcIdx ++, destIdx ++) {
        doubleVector[destIdx] = on.doubleVector[srcIdx];
      }
      destIdx += step;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Strings set
  //////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void putStringsRepeat(ColumnVector src, int srcPos, int destPos, int repeat) {
    OnColumnVector on = (OnColumnVector) src;
    byte[] bytes = on.bytesVector[srcPos];
    int len = on.lengths[srcPos];
    for (int i = 0; i < repeat; i ++) {
      bytesVector[destPos + i] = bytes.clone();
      starts[destPos + i] = 0;
      lengths[destPos + i] = len;
    }
  }

  @Override
  public void putStringsRepeats(ColumnVector src, int srcPos, int destPos, int repeat, int length) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < length; i ++) {
      byte[] value = on.bytesVector[srcPos + i];
      int len = value.length;
      for (int j = 0; j < repeat; j ++, destIdx ++) {
        bytesVector[destIdx] = value.clone();
        starts[destIdx] = 0;
        lengths[destIdx] = len;
      }
    }
  }

  @Override
  public void putStringsRun(ColumnVector src, int srcPos, int destPos, int length) {
    OnColumnVector on = (OnColumnVector) src;
    for (int i = 0; i < length; i ++) {
      bytesVector[destPos + i] = on.bytesVector[srcPos + i].clone();
      starts[destPos + i] = 0;
      lengths[destPos + i] = bytesVector[destPos + i].length;
    }
  }

  @Override
  public void putStringsRuns(ColumnVector src, int srcPos, int destPos, int repeat, int length) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < repeat; i ++) {
      int srcIdx = srcPos;
      for (int j = 0; j < length; j ++, srcIdx ++, destIdx ++) {
        bytesVector[destIdx] = on.bytesVector[srcIdx].clone();
        starts[destIdx] = 0;
        lengths[destIdx] = bytesVector[destIdx].length;
      }
    }
  }

  @Override
  public void putStringsRunsWithStep(ColumnVector src, int srcPos, int destPos, int repeat, int length, int step) {
    OnColumnVector on = (OnColumnVector) src;
    int destIdx = destPos;
    for (int i = 0; i < repeat; i ++) {
      int srcIdx = srcPos;
      for (int j = 0; j < length; j ++, srcIdx ++, destIdx ++) {
        bytesVector[destIdx] = on.bytesVector[srcIdx].clone();
        starts[destIdx] = 0;
        lengths[destIdx] = bytesVector[destIdx].length;
      }
      destIdx += step;
    }
  }

  /** Set a field by reference.
   *
   * @param elementNum index within column vector to set
   * @param sourceBuf container of source data
   * @param start start byte position within source
   * @param length  length of source byte sequence
   */
  public void setRef(int elementNum, byte[] sourceBuf, int start, int length) {
    this.bytesVector[elementNum] = sourceBuf;
    this.starts[elementNum] = start;
    this.lengths[elementNum] = length;
  }

  @Override
  public int getInt(int rowId) {
    return intVector[rowId];
  }

  @Override
  public long getLong(int rowId) {
    return longVector[rowId];
  }

  @Override
  public double getDouble(int rowId) {
    return doubleVector[rowId];
  }

  @Override
  public UTF8String getString(int rowId) {
    return str.update(bytesVector[rowId], starts[rowId], lengths[rowId]);
  }

  @Override
  public UTF8String getAnotherString(int rowId) {
    return anotherStr.update(bytesVector[rowId], starts[rowId], lengths[rowId]);
  }

  @Override
  public int getStart(int rowId) {
    return starts[rowId];
  }

  @Override
  public int getLength(int rowId) {
    return lengths[rowId];
  }

  @Override
  public void reset() {
    isRepeating = false;
    if (false == noNulls && isNull != null) {
      Arrays.fill(isNull, false);
      noNulls = true;
    }
    if (bytesVector != null) {
      Arrays.fill(starts, 0);
      Arrays.fill(lengths, 0);
    }
  }

  @Override
  public void free() {
    isNull = null;
    intVector = null;
    doubleVector = null;
    longVector = null;
    if (bytesVector != null) {
      for (int i = 0; i < bytesVector.length; i ++) {
        bytesVector[i] = null;
      }
      bytesVector = null;
      starts = null;
      lengths = null;
    }
  }

  @Override
  public long memoryFootprintInBytes() {
    long mem = 64 /* field size */+ capacity * 1 + 16 /* isNull array*/;
    if (dataType instanceof IntegerType) {
      mem += 4 * capacity + 16;
    } else if (dataType instanceof LongType) {
      mem += 8 * capacity + 16;
    } else if (dataType instanceof DoubleType) {
      mem += 8 * capacity + 16;
    } else if (dataType instanceof StringType) {
      mem += 4 * capacity + 16;
      mem += 4 * capacity + 16;
      mem += 4 * capacity + 16 + (16 + DEFAULT_STR_LEN) * capacity;
    }
    return mem;
  }
}
