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
package org.apache.spark.sql.execution.vector.sort4

import java.io.IOException

import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.{GenerateBatchCopier, InterBatchOrdering}
import org.apache.spark.sql.catalyst.vector.{IntComparator, OffHeapIntArrayTimSort, RowBatch}
import org.apache.spark.sql.execution.vector.sort.{EmptyRowBatchSorterIterator, RowBatchSorterIterator}
import org.apache.spark.unsafe.Platform
import scala.collection.mutable

object InMemoryBatchSorter4 {
  val MASK_INT_LOWER_16_BITS: Int = 0xFFFF;
  val MASK_INT_UPPER_16_BITS: Int = ~MASK_INT_LOWER_16_BITS;
}
case class InMemoryBatchSorter4(
                                 consumer: MemoryConsumer,
                                 interBatchOrdering: InterBatchOrdering,
                                 schema: Seq[Attribute],
                                 defaultCapacity: Int) {
  import InMemoryBatchSorter4._

  val sortedBatches: mutable.ArrayBuffer[RowBatch] = mutable.ArrayBuffer.empty[RowBatch]
  // var all: Array[Int] = null
  var all: Long = 0
  var starts: Array[Int] = null
  var lengths: Array[Int] = null
  var batchCount: Int = 0
  var totalSize: Int = 0

  val taskMemoryManager = TaskContext.get().taskMemoryManager()
  val allocateGranularity: Long = 16 * 1024 * 1024; // 16 MB

  var allocated: Long = 0
  var firstTime: Boolean = true

  val comparator: IntComparator = new IntComparator {
    override def compare(i1: Int, i2: Int): Int = {
      val b1: Int = (i1 & MASK_INT_UPPER_16_BITS) >>> 16
      val l1: Int = i1 & MASK_INT_LOWER_16_BITS
      val b2: Int = (i2 & MASK_INT_UPPER_16_BITS) >>> 16
      val l2: Int = i2 & MASK_INT_LOWER_16_BITS
      interBatchOrdering.compare(sortedBatches(b1), l1, sortedBatches(b2), l2)
    }
  }

  def insertBatch(rb: RowBatch): Unit = {
    // sortedBatches是一个大的ArrayBuffer，里面存的sortedRowBatch
    sortedBatches += rb
    batchCount += 1
    totalSize += rb.size

    if (allocated <= 0) {
      // 如果allocated<0，进行内存的申请，每次申请16M
      taskMemoryManager.acquireExecutionMemory(allocateGranularity,consumer)
      consumer.addUsed(allocateGranularity)
      allocated = allocateGranularity
    }
    // rb.size[存具体的列数据，每个数据4Byte]+2[1个存放列null的数目，1个存放null的位置]，3*16[行数，列数，列null的数目]
    val arraySize = (rb.size + 2) * 4 + (if (firstTime) {firstTime = false; 3 * 16} else 0)
    if (allocated > arraySize) {
      allocated -= arraySize
    } else {
      val short = arraySize - allocated
      taskMemoryManager.acquireExecutionMemory(allocateGranularity,consumer)
      consumer.addUsed(allocateGranularity)
      allocated = allocateGranularity - short
    }
  }

  def arrayFootprint(): Long = {
    if (sortedBatches.isEmpty) return 0
    val actualSize = 16 + totalSize * 4 + (16 + batchCount * 4) * 2
    val numAllocation = actualSize / allocateGranularity +
      (if (actualSize % allocateGranularity != 0) 1 else 0)
    numAllocation * allocateGranularity
  }

  def numBatches(): Int = sortedBatches.size

  def getMemoryUsage(): Long = {
    if (sortedBatches.isEmpty) return 0

    val arraySize = arrayFootprint()
    val batchSize = sortedBatches.head.memoryFootprintInBytes()
    val numBatchPerAllocation: Long = TaskMemoryManager.getNumBatchPerAllocation(batchSize)
    val granularity = batchSize * numBatchPerAllocation
    var allocationCount: Long = sortedBatches.size / numBatchPerAllocation
    if (sortedBatches.size % numBatchPerAllocation != 0) {
      allocationCount += 1
    }

    allocationCount * granularity + arraySize
  }

  def freeMemory(): Unit = {
    batchCount = 0
    totalSize = 0
    firstTime = true
    allocated = 0
    Platform.freeMemory(all)
    all = 0
    // all = null
    starts = null
    lengths = null

    var i = 0
    while (i < sortedBatches.size) {
      val rb = sortedBatches(i)
      rb.free()
      i += 1
    }
    sortedBatches.clear()
  }

  def preSort(): Unit = {
    all = Platform.allocateMemory(totalSize * 4)// new Array[Int](totalSize)
    starts = new Array[Int](batchCount)
    lengths = new Array[Int](batchCount)

    var pos = 0
    var rbIdx = 0
    while (rbIdx < batchCount) {
      val rb = sortedBatches(rbIdx)
      starts(rbIdx) = pos
      lengths(rbIdx) = rb.size

      val higherBits = rbIdx << 16
      var rowIdx = 0
      while (rowIdx < rb.size) {
        Platform.putInt(
          null, all + pos * 4, higherBits | (rowIdx & MASK_INT_LOWER_16_BITS))
        // all(pos) = higherBits | (rb.sorted(rowIdx) & 65535)

        pos += 1
        rowIdx += 1
      }

      rbIdx += 1
    }
  }

  def sort(): Unit = {
    preSort()
    OffHeapIntArrayTimSort.msort(all, totalSize, 0, totalSize, comparator, starts, lengths)
    // IntArrayTimSort.msort(all, 0, all.length, comparator, starts, lengths)
  }

  val batchCopier = GenerateBatchCopier.generate(schema, defaultCapacity)

  def getSortedIterator(): RowBatchSorterIterator = {
    if (totalSize == 0) return EmptyRowBatchSorterIterator

    // 多个Batch的部分merge
    sort()

    new RowBatchSorterIterator {

      val rb: RowBatch = RowBatch.create(schema.map(_.dataType).toArray, defaultCapacity)
      var i: Int = 0
      val total: Int = totalSize // all.length

      override def hasNext(): Boolean = i < total

      @throws[IOException]
      override def loadNext(): Unit = {
        rb.reset()
        var rowIdx: Int = 0

        while (rowIdx < defaultCapacity && i < total) {
          val rbAndRow = Platform.getInt(null, all + i * 4) // all(i)
          val batch = (rbAndRow & MASK_INT_UPPER_16_BITS) >>> 16
          val line = rbAndRow & MASK_INT_LOWER_16_BITS

          batchCopier.copy(sortedBatches(batch), line, rb, rowIdx)

          i += 1
          rowIdx += 1
        }
      }

      override def currentBatch: RowBatch = rb

      override def clone(): AnyRef = {
        new RowBatchSorterIterator {

          val newRB: RowBatch =
            RowBatch.create(schema.map(_.dataType).toArray, defaultCapacity, MemoryMode.ON_HEAP)
          var j: Int = i

          override def hasNext(): Boolean = j < total

          @throws[IOException]
          override def loadNext(): Unit = {
            newRB.reset()
            var rowIdx: Int = 0

            while (rowIdx < defaultCapacity && j < total) {
              val rbAndRow = Platform.getInt(null, all + j * 4) // all(j)
              val batch = (rbAndRow & MASK_INT_UPPER_16_BITS) >>> 16
              val line = rbAndRow & MASK_INT_LOWER_16_BITS

              batchCopier.copy(sortedBatches(batch), line, newRB, rowIdx)

              j += 1
              rowIdx += 1
            }
          }

          override def currentBatch: RowBatch = newRB
        }
      }
    }
  }
}

