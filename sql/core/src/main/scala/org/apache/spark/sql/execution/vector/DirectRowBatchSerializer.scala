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

package org.apache.spark.sql.execution.vector

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}

import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.{BatchRead, GenerateBatchRead}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

class DirectRowBatchSerializer(
    schema: Seq[Attribute],
    defaultCapacity: Int,
    shouldReuseBatch: Boolean,
    parentId: Int) extends Serializer with Serializable {

  override def newInstance(): SerializerInstance =
    new DirectRowBatchSerializerInstance(
      schema, defaultCapacity, shouldReuseBatch, parentId)
  override private[spark] def supportsRelocationOfSerializedObjects: Boolean = true
}

private class DirectRowBatchSerializerInstance(
    schema: Seq[Attribute],
    defaultCapacity: Int,
    shouldReuseBatch: Boolean,
    parentId: Int) extends SerializerInstance {

  /**
    * Serializes a stream of UnsafeRows. Within the stream, each record consists of a record
    * length (stored as a 4-byte integer, written high byte first), followed by the record's bytes.
    */
  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))
    private[this] val wbc: WritableByteChannel = Channels.newChannel(dOut)

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val rb = value.asInstanceOf[RowBatch]

      if (rb.writer != null) {
        dOut.writeInt(rb.numRows)
        dOut.writeInt(schema.size)
        rb.writeToStreamInRange(wbc)
      } else { // columns are stored serialized in SerDeHelper
        dOut.writeInt(rb.size)
        dOut.writeInt(schema.size)
        rb.writeBuffers(wbc)
      }
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      dOut.close()
      wbc.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    if (shouldReuseBatch) {
      new DeserializationStream {
        private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))
        private[this] var rowBatch: RowBatch =
          RowBatch.create(schema.map(_.dataType).toArray, defaultCapacity)
        private[this] var batchTuple: (Int, RowBatch) = (0, rowBatch)
        private[this] val reader: BatchRead = GenerateBatchRead.generate(schema, defaultCapacity)
        private[this] val rbc: ReadableByteChannel = Channels.newChannel(dIn)
        rowBatch.reader = reader

        private[this] val EOF: Int = -1

        override def asKeyValueIterator: Iterator[(Int, RowBatch)] = {
          new Iterator[(Int, RowBatch)] {

            private[this] def readSize(): Int = try {
              dIn.readInt()
            } catch {
              case e: EOFException =>
                dIn.close()
                EOF
            }

            private[this] var nextBatchSize: Int = readSize()
            override def hasNext: Boolean = nextBatchSize != EOF

            override def next(): (Int, RowBatch) = {
              if (rowBatch.capacity < nextBatchSize) {
                rowBatch = RowBatch.create(schema.map(_.dataType).toArray, nextBatchSize)
                rowBatch.reader = reader
              }
              rowBatch.reset()
              while (rowBatch.size + nextBatchSize <= rowBatch.capacity && nextBatchSize != EOF) {
                readSize() // read column num
                rowBatch.appendFromStream(rbc, nextBatchSize)
                nextBatchSize = readSize()
              }
              if (nextBatchSize == EOF) { // We are returning the last row in this stream
                dIn.close()
                val _batchTuple = batchTuple
                // Null these out so that the byte array can be garbage collected once the entire
                // iterator has been consumed
                rowBatch = null
                batchTuple = null
                _batchTuple
              } else {
                batchTuple
              }
            }
          }
        }

        override def asIterator: Iterator[Any] = {
          // This method is never called by shuffle code.
          throw new UnsupportedOperationException
        }

        override def readKey[T: ClassTag](): T = {
          // We skipped serialization of the key in writeKey(), so just return a dummy value since
          // this is going to be discarded anyways.
          null.asInstanceOf[T]
        }

        override def readValue[T: ClassTag](): T = {
          throw new UnsupportedOperationException
        }

        override def readObject[T: ClassTag](): T = {
          // This method is never called by shuffle code.
          throw new UnsupportedOperationException
        }

        override def close(): Unit = {
          dIn.close()
          rbc.close()
        }
      }
    } else { // shouldn't reuse row batch
      new DeserializationStream {
        private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))
        private[this] var rowBatch: RowBatch = null
        private[this] val reader: BatchRead = GenerateBatchRead.generate(schema, defaultCapacity)
        private[this] val rbc: ReadableByteChannel = Channels.newChannel(dIn)
        private[this] val dts: Array[DataType] = schema.map(_.dataType).toArray
        private[this] val estimatedBatchSize: Long =
          RowBatch.estimateMemoryFootprint(dts, defaultCapacity, MemoryMode.OFF_HEAP)
        private[this] val taskContext: TaskContext = TaskContext.get()
        private[this] val taskMemoryManager: TaskMemoryManager = taskContext.taskMemoryManager()
        private[this] val consumer: MemoryConsumer =
          taskContext.getMemoryConsumer(parentId).asInstanceOf[MemoryConsumer]

        private[this] val allocateGranularity: Long =
          TaskMemoryManager.getAllocateGranularity(estimatedBatchSize)

        private[this] val EOF: Int = -1

        override def asKeyValueIterator: Iterator[(Int, RowBatch)] = {
          new Iterator[(Int, RowBatch)] {

            private var numBatch: Long = 0L

            private[this] def readSize(): Int = try {
              dIn.readInt()
            } catch {
              case e: EOFException =>
                dIn.close()
                EOF
            }

            private[this] var nextBatchSize: Int = readSize()
            override def hasNext: Boolean = nextBatchSize != EOF

            override def next(): (Int, RowBatch) = {
              if (consumer.getAllocated() >= estimatedBatchSize) {
                consumer.minusAllocated(estimatedBatchSize)
              } else {
                taskMemoryManager.acquireExecutionMemory(
                  allocateGranularity,consumer)
                consumer.addUsed(allocateGranularity);
                consumer.setAllocated(allocateGranularity - estimatedBatchSize)
              }

              rowBatch = RowBatch.create(dts, defaultCapacity, MemoryMode.OFF_HEAP)
              numBatch += 1L

              rowBatch.reader = reader
              rowBatch.reset()

              while (rowBatch.size + nextBatchSize <= rowBatch.capacity && nextBatchSize != EOF) {
                readSize() // read column num
                rowBatch.appendFromStream(rbc, nextBatchSize)
                nextBatchSize = readSize()
              }
              (0, rowBatch)
            }
          }
        }

        override def asIterator: Iterator[Any] = {
          // This method is never called by shuffle code.
          throw new UnsupportedOperationException
        }

        override def readKey[T: ClassTag](): T = {
          // We skipped serialization of the key in writeKey(), so just return a dummy value since
          // this is going to be discarded anyways.
          null.asInstanceOf[T]
        }

        override def readValue[T: ClassTag](): T = {
          throw new UnsupportedOperationException
        }

        override def readObject[T: ClassTag](): T = {
          // This method is never called by shuffle code.
          throw new UnsupportedOperationException
        }

        override def close(): Unit = {
          dIn.close()
          rbc.close()
        }
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
