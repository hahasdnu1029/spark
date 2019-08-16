/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.vector;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.vector.BatchRead;
import org.apache.spark.sql.catalyst.expressions.vector.BatchWrite;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;

public class RowBatch implements Serializable {
  public int numCols; // number of columns
  public int capacity;
  public int size; // number of rows that qualify
  public int[] selected; // array of selected rows
  public boolean selectedInUse; // if selected is valid
  public ColumnVector[] columns;
  public ColumnVectorSerDeHelper[] buffers;

  // 保存的待排序的行索引
  public int[] sorted; // array of sorted row indices
  public boolean sortedInUse; // if sorted is valid
  public int startIdx;
  public int numRows;

  public int rowIdx = 0; // row idx while iterate this batch

  public int id = 0; // for sort and get

  public BatchWrite writer;
  public BatchRead reader;

  public DataType[] fieldTypes;
  public List<String> colNames;

  public UTF8String str = new UTF8String();

  public boolean endOfFile;

  public static final String SPARK_SQL_VECTORIZE_BATCH_CAPACITY = "spark.sql.vectorize.batch.capacity";

  public static final int DEFAULT_CAPACITY = 1024;

  /**
   * Constructor for serialization purpose only
   * @param dataTypes
   */
  public RowBatch(DataType[] dataTypes, int capacity) {
    this.capacity = capacity;
    this.numCols = dataTypes.length;
    this.size = 0;
    this.selectedInUse = false;
    this.sortedInUse = false;
    this.fieldTypes = dataTypes;
    this.endOfFile = false;
    this.buffers = new ColumnVectorSerDeHelper[numCols];
    for (int i = 0; i < numCols; i ++) {
      buffers[i] = new ColumnVectorSerDeHelper(dataTypes[i], capacity);
    }
  }

  public RowBatch(int numCols, int capacity) {
    this.numCols = numCols;
    this.capacity = capacity;
    size = capacity;
    selected = new int[capacity];
    selectedInUse = false;
    sorted = new int[capacity];
    sortedInUse = false;
    columns = new ColumnVector[numCols];
  }

  /**
   * The caller of this ctor doesn't care about batch size,
   * the newly created batch act as a ColumnVector holder.
   */
  public RowBatch(int numCols) {
    //this(numCols, DEFAULT_CAPACITY);
    this.numCols = numCols;
    this.capacity = 0;
    this.size = 0;
    this.selectedInUse = false;
    this.sortedInUse = false;
    columns = new ColumnVector[numCols];
  }

  public static RowBatch create(DataType[] dts, int capacity) {
    return create(dts, capacity, MemoryMode.ON_HEAP);
  }

  public static RowBatch create(DataType[] dts, int capacity, MemoryMode mode) {
    RowBatch rb = new RowBatch(dts.length, capacity);
    rb.fieldTypes = dts;
    if (mode == MemoryMode.ON_HEAP) {
      for (int i = 0; i < dts.length; i ++) {
        rb.columns[i] = new OnColumnVector(dts[i], capacity);
      }
    } else {
      for (int i = 0; i < dts.length; i ++) {
        rb.columns[i] = new OffColumnVector(dts[i], capacity);
      }
    }
    return rb;
  }

  public static RowBatch create2(DataType[] dts, int capacity, MemoryMode mode) {
    RowBatch rb = new RowBatch(dts.length);
    rb.fieldTypes = dts;
    if (mode == MemoryMode.ON_HEAP) {
      for (int i = 0; i < dts.length; i ++) {
        rb.columns[i] = new OnColumnVector(dts[i], capacity);
      }
    } else {
      for (int i = 0; i < dts.length; i ++) {
        rb.columns[i] = new OffColumnVector(dts[i], capacity);
      }
    }
    return rb;
  }

  public static RowBatch createWithName(DataType[] dts, int capacity, List<String> colNames) {
    RowBatch rb = create(dts, capacity, MemoryMode.ON_HEAP);
    rb.colNames = colNames;
    return rb;
  }

  public void reset() {
    selectedInUse = false;
    sortedInUse = false;
    size = 0;
    endOfFile = false;
    if (columns != null) {
      for (ColumnVector col : columns) {
        col.reset();
      }
    }
    if (buffers != null) {
      for (ColumnVectorSerDeHelper buffer: buffers) {
        buffer.reset();
      }
    }
  }

  public static long estimateMemoryFootprint(DataType[] dataTypes, int capacity, MemoryMode mode) {
    long mem = 12 /* object header*/ + 6 * 4 + 3 * 1 + 1 /*pad*/ + 9 * 4 + 4 /*pad*/;
    mem += 16 + 4 * 10 /* cv array*/ + 16 + capacity * 4 /* selected*/ + 16 + capacity * 4 /* sorted*/;
    for (DataType dt: dataTypes) {
      mem += ColumnVector.estimateMemoryFootprint(dt, capacity, mode);
    }
    return mem;
  }

  public long memoryFootprintInBytes() {
    long mem = 12 /* object header*/ + 6 * 4 + 3 * 1 + 1 /*pad*/ + 9 * 4 + 4 /*pad*/;
    mem += 16 + 4 * 10 /* cv array*/ + 16 + capacity * 4 /* selected*/ + 16 + capacity * 4 /* sorted*/;
    for (ColumnVector cv: columns) {
      mem += cv.memoryFootprintInBytes();
    }
    return mem;
  }

  public void free() {
    selected = null;
    sorted = null;
    for (ColumnVector cv: columns) {
      cv.free();
    }
  }

  public void writeBuffers(WritableByteChannel out) throws IOException {
    for (ColumnVectorSerDeHelper buffer : buffers) {
      buffer.writeBuffers(out);
    }
  }

  public void appendFromStream(ReadableByteChannel in, int numRows) throws IOException {
    reader.append(in, this, size, numRows);
    size += numRows;
  }

  public void writeToStreamInRange(WritableByteChannel out) throws IOException {
    writer.write(this, out);
  }

  public int[] getSelected() {
    if (selectedInUse) {
      return selected;
    } else {
      int[] newSelected = new int[size]; // TODO: should we update the rowbatch's one
      for (int i = 0; i < size; i ++) {
        newSelected[i] = i;
      }
      return newSelected;
    }
  }

  public Iterator<Row> rowIterator() {
    final Row row = new Row();
    return new Iterator<Row>() {
      int idxInSelectedArray = 0;
      int[] selected = getSelected();

      @Override
      public boolean hasNext() {
        return idxInSelectedArray < size;
      }

      @Override
      public Row next() {
        row.rowId = selected[idxInSelectedArray];
        idxInSelectedArray += 1;
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public void sort(IntComparator comparator) {
    sortedInUse = true;
    if (selectedInUse) {
      for (int i = 0; i < size; i ++) {
        sorted[i] = selected[i];
      }
    } else {
      for (int i = 0; i < size; i ++) {
        sorted[i] = i;
      }
    }
    IntArrayTimSort.sort(sorted, 0, size, comparator, null, 0, 0);
  }

  public void sort(final int[] sortedBy) {
    IntComparator comparator =
        new IntComparator() {
          @Override
          public int compare(int i1, int i2) {
            int x = sortedBy[i1];
            int y = sortedBy[i2];
            return (x < y) ? -1 : ((x == y) ? 0 : 1);
          }
        };
    sort(comparator);
  }

  public final class Row extends InternalRow {
    private int rowId;

    @Override
    public int numFields() {
      return numCols;
    }

    @Override
    public void setNullAt(int i) {

    }

    @Override
    public void update(int i, Object value) {

    }

    @Override
    public InternalRow copy() {
      Object[] arr = new Object[numCols];
      for (int i = 0; i < numCols; i ++) {
        if (columns[i].dataType instanceof IntegerType) {
          arr[i] = getInt(i);
        } else if (columns[i].dataType instanceof LongType) {
          arr[i] = getLong(i);
        } else if (columns[i].dataType instanceof DoubleType) {
          arr[i] = getDouble(i);
        } else if (columns[i].dataType instanceof StringType) {
          arr[i] = getUTF8String(i).clone();
        }
      }
      return new GenericInternalRow(arr);
    }

    @Override
    public boolean anyNull() {
      for (int i = 0; i < numFields(); i ++) {
        if (isNullAt(i)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return columns[ordinal].isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public byte getByte(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public short getShort(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public int getInt(int ordinal) {
      return columns[ordinal].getInt(rowId);
    }

    @Override
    public long getLong(int ordinal) {
      return columns[ordinal].getLong(rowId);
    }

    @Override
    public float getFloat(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public double getDouble(int ordinal) {
      return columns[ordinal].getDouble(rowId);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      throw new NotImplementedException();
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return columns[ordinal].getString(rowId);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      throw new NotImplementedException();
    }

    @Override
    public ArrayData getArray(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public MapData getMap(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      throw new NotImplementedException();
    }
  }
}
