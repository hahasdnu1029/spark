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
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.{GenerateBatchReorderer, InterBatchOrdering}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.vector.sort.{RowBatchSorterIterator, RowBatchSpillMerger, RowBatchSpillWriter}
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.{TaskCompletionListener, Utils}
import org.apache.spark.internal.Logging

import scala.collection.mutable

class ExternalBatchSorter4(
    taskMemoryManager: TaskMemoryManager,
    blockManager: BlockManager,
    taskContext: TaskContext,
    interBatchComparator: InterBatchOrdering,
    schema: Seq[Attribute],
    defaultCapacity: Int)
  extends MemoryConsumer(taskMemoryManager, taskMemoryManager.pageSizeBytes, MemoryMode.OFF_HEAP) with Logging {

  val spillWriters = mutable.ArrayBuffer.empty[RowBatchSpillWriter]

  var inMemoryBatchSorter: InMemoryBatchSorter4 = null
  inMemoryBatchSorter = InMemoryBatchSorter4(this, interBatchComparator, schema, defaultCapacity)

  private var readingIterator: SpillableBatchIterator = null

  var peakMemoryUsedBytes: Long = 0L

  val writeMetrics = new ShuffleWriteMetrics()

  // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
  // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
  // does not fully consume the sorter's output (e.g. sort followed by limit).
  taskContext.addTaskCompletionListener(new TaskCompletionListener() {
    def onTaskCompletion(context: TaskContext) {
      cleanupResources()
    }
  })

  val schemaArray = schema.map(_.dataType).toArray

  val estimatedBatchSize: Long = RowBatch.estimateMemoryFootprint(
    schemaArray, defaultCapacity, MemoryMode.OFF_HEAP)

  val allocateGranularity: Long = TaskMemoryManager.getAllocateGranularity(estimatedBatchSize)

  def acquireMemory(): Unit = {
    if (getAllocated() >= estimatedBatchSize) {
      minusAllocated(estimatedBatchSize);
    } else {
      acquireMemory(allocateGranularity)
    }
  }

  val reorderer = GenerateBatchReorderer.generate(schema, defaultCapacity)

  def insertBatch(rb: RowBatch): Unit = {
    acquireMemory()
    val sortedRB = RowBatch.create2(schemaArray, defaultCapacity, MemoryMode.OFF_HEAP)
    sortedRB.capacity = rb.capacity
    sortedRB.size = rb.size
    sortedRB.endOfFile = rb.endOfFile
    reorderer.copy(rb, sortedRB)
    inMemoryBatchSorter.insertBatch(sortedRB)
  }

  def getSortedIterator(): RowBatchSorterIterator = {
    if (spillWriters.isEmpty) {
      assert(inMemoryBatchSorter != null)
      readingIterator = new SpillableBatchIterator(inMemoryBatchSorter.getSortedIterator())
      readingIterator
    } else {
      val num = spillWriters.size + (if (inMemoryBatchSorter != null) 1 else 0)
      val spillMerger =
        new RowBatchSpillMerger(interBatchComparator, num, schema, defaultCapacity)
      for (writer <- spillWriters) {
        spillMerger.addSpillIfNotEmpty(writer.getReader(blockManager))
      }
      if (inMemoryBatchSorter != null) {
        readingIterator = new SpillableBatchIterator(inMemoryBatchSorter.getSortedIterator())
        spillMerger.addSpillIfNotEmpty(readingIterator)
      }
      spillMerger.getSortedIterator()
    }
  }


  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this) {
      if (readingIterator != null) {
         return readingIterator.spill()
       }
      return 0
    }

    if (inMemoryBatchSorter == null || inMemoryBatchSorter.numBatches() <= 0) {
      return 0
    }

    logInfo(s"Thread ${Thread.currentThread.getId} spilling sort data of " +
      s"${Utils.bytesToString(getMemoryUsage())} to disk (${spillWriters.size} times so far)")

    if (inMemoryBatchSorter.numBatches() > 0) {
      val spillWriter: RowBatchSpillWriter =
        new RowBatchSpillWriter(blockManager, writeMetrics, schema, defaultCapacity)
      spillWriters += spillWriter
      val sortedBatches: RowBatchSorterIterator = inMemoryBatchSorter.getSortedIterator()
      while (sortedBatches.hasNext) {
        sortedBatches.loadNext()
        spillWriter.write(sortedBatches.currentBatch)
      }
      spillWriter.close()
    }

    val spillSize: Long = freeMemory()
    taskContext.taskMetrics.incMemoryBytesSpilled(spillSize)

    return spillSize
  }

  def freeMemory(): Long = {
    val memoryFreed = getMemoryUsage()
    if (memoryFreed > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = memoryFreed
    }
    used -= memoryFreed

    if (inMemoryBatchSorter != null) {
      inMemoryBatchSorter.freeMemory()
    }
    taskMemoryManager.releaseExecutionMemory(memoryFreed, this)
    memoryFreed
  }

  def getMemoryUsage(): Long = {
     if (inMemoryBatchSorter == null) 0 else inMemoryBatchSorter.getMemoryUsage()
  }

  def updatePeakMemoryUsed(): Unit = {
    val mem = getMemoryUsage()
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem
    }
  }

  def peakMemoryUsage(): Long = {
    updatePeakMemoryUsed()
    peakMemoryUsedBytes
  }

  def cleanupResources(): Unit = {
    this.synchronized {
      deleteSpillFiles()
      freeMemory()
      if (inMemoryBatchSorter != null) {
        inMemoryBatchSorter.freeMemory()
        inMemoryBatchSorter = null
      }
    }
  }

  def deleteSpillFiles(): Unit = {
    for (writer <- spillWriters) {
      val f = writer.file
      if (f != null && f.exists()) {
        if (!f.delete()) {
          logError(s"Was unable to delete spill file ${f.getAbsolutePath}")
        }
      }
    }
  }

  class SpillableBatchIterator(
      inMemSortedIterator: RowBatchSorterIterator) extends RowBatchSorterIterator {

    var upstream: RowBatchSorterIterator = inMemSortedIterator
    var nextUpstream: RowBatchSorterIterator = null

    override def hasNext(): Boolean = {
      if (nextUpstream != null) {
        nextUpstream.hasNext()
      } else {
        upstream.hasNext()
      }
    }

    @throws[IOException]
    override def loadNext(): Unit = {
      this.synchronized {
        if (nextUpstream != null) {
          upstream = nextUpstream
          nextUpstream = null
        }
        upstream.loadNext()
      }
    }

    override def currentBatch: RowBatch = upstream.currentBatch

    @throws[IOException]
    def spill(): Long = {
      this.synchronized {
        if (!(nextUpstream == null && upstream.hasNext())) {
          return 0
        }

        val spillWriter: RowBatchSpillWriter =
          new RowBatchSpillWriter(blockManager, writeMetrics, schema, defaultCapacity)

        // clone the iterator to avoid changing the currentBatch in use by PriorityQueue
        val writes: RowBatchSorterIterator = upstream.clone().asInstanceOf[RowBatchSorterIterator]
        while (writes.hasNext()) {
          writes.loadNext()
          spillWriter.write(writes.currentBatch)
        }
        spillWriter.close()

        spillWriters += spillWriter
        nextUpstream = spillWriter.getReader(blockManager)

        var released: Long = 0
        ExternalBatchSorter4.this.synchronized {
          released += freeMemory()
        }
        released
      }
    }
  }

}
