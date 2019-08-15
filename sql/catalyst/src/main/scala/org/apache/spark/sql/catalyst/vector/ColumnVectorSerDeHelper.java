package org.apache.spark.sql.catalyst.vector;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class ColumnVectorSerDeHelper {

  protected DataType dt;
  protected int capacity;

  protected ByteBuffer values;
  protected ByteBuffer nulls;

  protected boolean noNulls;
  protected int nullCount;
  protected int notNullCount;
  protected int pos;

  public static ColumnVectorSerDeHelper createIntBuffer(int capacity) {
    return new ColumnVectorSerDeHelper(IntegerType$.MODULE$, capacity);
  }

  public static ColumnVectorSerDeHelper createLongBuffer(int capacity) {
    return new ColumnVectorSerDeHelper(LongType$.MODULE$, capacity);
  }

  public static ColumnVectorSerDeHelper createDoubleBuffer(int capacity) {
    return new ColumnVectorSerDeHelper(DoubleType$.MODULE$, capacity);
  }

  public static ColumnVectorSerDeHelper createStringBuffer(int capacity) {
    return new ColumnVectorSerDeHelper(StringType$.MODULE$, capacity);
  }

  public ColumnVectorSerDeHelper(DataType type, int capacity) {
    prepareBuffers(type, capacity);
  }

  private ByteBuffer fourByte = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());

  private void initialState() {
    this.nullCount = 0;
    this.notNullCount = 0;
    this.pos = 0;
  }

  private void prepareBuffers(DataType dt, int capacity) {
    this.capacity = capacity;
    this.dt = dt;
    initialState();
    if (nulls == null) {
      nulls = ByteBuffer.allocate(1024);
      nulls.order(ByteOrder.nativeOrder());
    } else {
      nulls.clear();
    }
    if (values == null) {
      values = ByteBuffer.allocate(capacity * (dt instanceof IntegerType ? 4 : 8));
      values.order(ByteOrder.nativeOrder());
    } else {
      values.clear();
    }
  }

  public void reset() {
    initialState();
    if (nulls != null) {
      nulls.clear();
    }
    if (values != null) {
      values.clear();
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Write part of (sorted[], from, length) the ColumnVector to stream
  ///////////////////////////////////////////////////////////////////////////////////////////////

  public void writeIntCV(ColumnVector cv, int[] positions, int from, int length, WritableByteChannel out) throws IOException {
    prepareBuffers(IntegerType$.MODULE$, length);
    serializeIntCV(cv, positions, from, length);
    writeBuffers(out);
  }

  public void writeLongCV(ColumnVector cv, int[] positions, int from, int length, WritableByteChannel out) throws IOException {
    prepareBuffers(LongType$.MODULE$, length);
    serializeLongCV(cv, positions, from, length);
    writeBuffers(out);
  }

  public void writeDoubleCV(ColumnVector cv, int[] positions, int from, int length, WritableByteChannel out) throws IOException {
    prepareBuffers(DoubleType$.MODULE$, length);
    serializeDoubleCV(cv, positions, from, length);
    writeBuffers(out);
  }

  public void writeStringCV(ColumnVector cv, int[] positions, int from, int length, WritableByteChannel out) throws IOException {
    prepareBuffers(StringType$.MODULE$, length);
    serializeStringCV(cv, positions, from, length);
    writeBuffers(out);
  }

  private void writeInt(int i, WritableByteChannel out) throws IOException {
    fourByte.clear();
    fourByte.putInt(i);
    fourByte.flip();
    out.write(fourByte);
  }

  public void writeBuffers(WritableByteChannel out) throws IOException {
    writeInt(nullCount, out);
    if (nullCount > 0) {
      nulls.flip();
      out.write(nulls);
    }
    values.flip();
    writeInt(values.limit(), out);
    out.write(values);
  }


  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Serialize part of (sorted[], from, length) the ColumnVector into the buffers
  ///////////////////////////////////////////////////////////////////////////////////////////////
  public void serializeIntCV(ColumnVector src, int[] positions, int from, int length) {
    values = ensureFreeSpace(values, length * 4);
    if (src.noNulls) {
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        values.putInt(src.getInt(i));
      }
      notNullCount += length;
      pos += length;
    } else {
      nulls = ensureFreeSpace(nulls, length * 4);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        if (src.isNullAt(i)) {
          nullCount += 1;
          nulls.putInt(pos);
        } else {
          notNullCount += 1;
          values.putInt(src.getInt(i));
        }
        pos += 1;
      }
    }
  }

  public void serializeLongCV(ColumnVector src, int[] positions, int from, int length) {
    values = ensureFreeSpace(values, length * 8);
    if (src.noNulls) {
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        values.putLong(src.getLong(i));
      }
      notNullCount += length;
      pos += length;
    } else {
      nulls = ensureFreeSpace(nulls, length * 4);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        if (src.isNullAt(i)) {
          nullCount += 1;
          nulls.putInt(pos);
        } else {
          notNullCount += 1;
          values.putLong(src.getLong(i));
        }
        pos += 1;
      }
    }
  }

  public void serializeDoubleCV(ColumnVector src, int[] positions, int from, int length) {
    values = ensureFreeSpace(values, length * 8);
    if (src.noNulls) {
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        values.putDouble(src.getDouble(i));
      }
      notNullCount += length;
      pos += length;
    } else {
      nulls = ensureFreeSpace(nulls, length * 4);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        if (src.isNullAt(i)) {
          nullCount += 1;
          nulls.putInt(pos);
        } else {
          notNullCount += 1;
          values.putDouble(src.getDouble(i));
        }
        pos += 1;
      }
    }
  }

  public void serializeStringCV(ColumnVector src, int[] positions, int from, int length) {
    values = ensureFreeSpace(values, length * 8);
    if (src.noNulls) {
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        int numBytes = src.getLength(i);
        values = ensureFreeSpace(values, 4 + numBytes);
        values.putInt(numBytes);
        serializeString(src, i, values);
      }
      notNullCount += length;
      pos += length;
    } else {
      nulls = ensureFreeSpace(nulls, length * 4);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        if (src.isNullAt(i)) {
          nullCount += 1;
          nulls.putInt(pos);
        } else {
          notNullCount += 1;
          int numBytes = src.getLength(i);
          values = ensureFreeSpace(values, 4 + numBytes);
          values.putInt(numBytes);
          serializeString(src, i, values);
        }
        pos += 1;
      }
    }
  }

  private void serializeString(ColumnVector src, int i, ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    int start = src.getStart(i);
    int numBytes = src.getLength(i);
    if (src instanceof OnColumnVector) {
      Platform.copyMemory(src.getBytesVector()[i], start + Platform.BYTE_ARRAY_OFFSET,
        target, Platform.BYTE_ARRAY_OFFSET + offset + pos, numBytes);
    } else {
      Platform.copyMemory(null, start + src.getDataNativeAddress(),
        target, Platform.BYTE_ARRAY_OFFSET + offset + pos, numBytes);
    }
    buffer.position(pos + numBytes);
  }


  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Read Stream into ColumnVector in range (startIdx, count)
  ///////////////////////////////////////////////////////////////////////////////////////////////

  public int populateIntCV(ColumnVector cv, ReadableByteChannel in, int startIdx, int count) throws IOException {
    int notNullCount = readNulls(in, startIdx, count, cv);
    if (noNulls) {
      for (int i = 0; i < notNullCount; i ++) {
        cv.putInt(i + startIdx, values.getInt());
      }
    } else {
      for (int i = 0; i < count; i ++) {
        if (!cv.isNullAt(i + startIdx)) {
          cv.putInt(i + startIdx, values.getInt());
        }
      }
    }
    return notNullCount;
  }

  public int populateLongCV(ColumnVector cv, ReadableByteChannel in, int startIdx, int count) throws IOException {
    int notNullCount = readNulls(in, startIdx, count, cv);
    if (noNulls) {
      for (int i = 0; i < notNullCount; i ++) {
        cv.putLong(i + startIdx, values.getLong());
      }
    } else {
      for (int i = 0; i < count; i ++) {
        if (!cv.isNullAt(i + startIdx)) {
          cv.putLong(i + startIdx, values.getLong());
        }
      }
    }
    return notNullCount;
  }

  public int populateDoubleCV(ColumnVector cv, ReadableByteChannel in, int startIdx, int count) throws IOException {
    int notNullCount = readNulls(in, startIdx, count, cv);
    if (noNulls) {
      for (int i = 0; i < notNullCount; i ++) {
        cv.putDouble(i + startIdx, values.getDouble());
      }
    } else {
      for (int i = 0; i < count; i ++) {
        if (!cv.isNullAt(i + startIdx)) {
          cv.putDouble(i + startIdx, values.getDouble());
        }
      }
    }
    return notNullCount;
  }

  public int populateStringCV(ColumnVector cv, ReadableByteChannel in, int startIdx, int count) throws IOException {
    int notNullCount = readNulls(in, startIdx, count, cv);
    if (cv instanceof OnColumnVector) {
      int[] lengths = cv.getLengthsVector();
      int[] starts = cv.getStartsVector();
      byte[][] bytesVector = cv.getBytesVector();
      if (noNulls) {
        for (int i = 0; i < notNullCount; i ++) {
          int j = i + startIdx;
          lengths[j] = values.getInt();
          starts[j] = 0;
          if (bytesVector[j] == null || bytesVector[j].length < lengths[j]) {
            bytesVector[j] = new byte[lengths[j]];
          }
          values.get(bytesVector[j], 0, lengths[j]);
        }
      } else {
        for (int i = 0; i < count; i ++) {
          int j = i + startIdx;
          if (!cv.isNullAt(j)) {
            lengths[j] = values.getInt();
            starts[j] = 0;
            if (bytesVector[j] == null || bytesVector[j].length < lengths[j]) {
              bytesVector[j] = new byte[lengths[j]];
            }
            values.get(bytesVector[j], 0, lengths[j]);
          }
        }
      }
    } else {
      OffColumnVector off = (OffColumnVector) cv;
      if (noNulls) {
        for (int i = 0; i < notNullCount; i ++) {
          int j = i + startIdx;
          int length = values.getInt();
          off.copyFromByteBuffer(j, values, length);
        }
      } else {
        for (int i = 0; i < count; i ++) {
          int j = i + startIdx;
          if (!cv.isNullAt(j)) {
            int length = values.getInt();
            off.copyFromByteBuffer(j, values, length);
          }
        }
      }
    }
    return notNullCount;
  }

  /**
   * @return notNullCount
   */
  private int readNulls(ReadableByteChannel in, int startIdx, int count, ColumnVector cv) throws IOException {
    prepareBuffers(cv.dataType, count);
    int nullCount = readInt(in);
    if (nullCount > 0) {
      nulls = ensureFreeSpace(nulls, nullCount * 4);
      nulls.limit(nullCount * 4);
      in.read(nulls);
      nulls.flip();
      cv.noNulls = noNulls = false;
      for (int i = 0; i < nullCount; i ++) {
        int nullPos = nulls.getInt();
        cv.putNull(nullPos + startIdx);
      }
    }
    int notNullCount = count - nullCount;
    int valueSize = readInt(in);
    values = ensureFreeSpace(values, valueSize);
    values.limit(valueSize);
    in.read(values);
    values.flip();
    return notNullCount;
  }

  private int readInt(ReadableByteChannel in) throws IOException {
    fourByte.clear();
    in.read(fourByte);
    return fourByte.getInt(0);
  }


  private ByteBuffer ensureFreeSpace(ByteBuffer orig, int size) {
    if (orig.remaining() >= size) {
      return orig;
    } else {
      // grow in steps of initial size
      int capacity = orig.capacity();
      int newSize = capacity + Math.max(size, capacity);
      int pos = orig.position();

      return ByteBuffer
          .allocate(newSize)
          .order(ByteOrder.nativeOrder())
          .put(orig.array(), 0, pos);
    }
  }

}
