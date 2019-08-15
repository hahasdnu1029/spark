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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.{BatchOrdering, InterBatchOrdering}
import org.apache.spark.sql.catalyst.util.AbstractScalaRowIterator
import org.apache.spark.sql.catalyst.vector.{IntComparator, RowBatch}
import org.apache.spark.sql.execution.vector.sort.RowBatchSorterIterator
import org.apache.spark.{SparkEnv, TaskContext}

case class ExternalRowBatchSorter4(
    output: Seq[Attribute],
    defaultCapacity: Int,
    innerBatchComparator: BatchOrdering,
    interBatchComparator: InterBatchOrdering) {

  val sparkEnv: SparkEnv = SparkEnv.get
  val taskContext: TaskContext = TaskContext.get

  val sorter: ExternalBatchSorter4 = new ExternalBatchSorter4(
    taskContext.taskMemoryManager,
    sparkEnv.blockManager,
    taskContext,
    interBatchComparator,
    output,
    defaultCapacity)

  var testSpillFrequency: Int = 0
  def setTestSpillFrequency(frequency: Int): Unit = {
    testSpillFrequency = frequency
  }

  var numBatchesInserted: Int = 0

  val innerBatchCmp = new IntComparator() {
    def compare(i1: Int, i2: Int): Int = innerBatchComparator.compare(i1, i2)
  }

  def insertBatch(rb: RowBatch): Unit = {

    innerBatchComparator.reset(rb)
    rb.sort(innerBatchCmp)

    sorter.insertBatch(rb)
    numBatchesInserted += 1

    if (testSpillFrequency > 0 && (numBatchesInserted % testSpillFrequency) == 0) {
      sorter.spill()
    }
  }

  def sort(iter: Iterator[RowBatch]): Iterator[RowBatch] = {
    while (iter.hasNext) {
      insertBatch(iter.next())
    }
    sort()
  }

  def sort(): Iterator[RowBatch] = {
    try {
      val sortedIterator: RowBatchSorterIterator = sorter.getSortedIterator()
      if (!sortedIterator.hasNext()) {
        cleanupResources()
      }

      new AbstractScalaRowIterator[RowBatch] {
        override def hasNext: Boolean = sortedIterator.hasNext()

        override def next(): RowBatch = {
          sortedIterator.loadNext()
          if (!hasNext) {
            cleanupResources()
          }
          sortedIterator.currentBatch
        }
      }

    } catch {
      case e: IOException =>
        cleanupResources()
        Iterator.empty
    }
  }

  def peakMemoryUsage(): Long = {
    sorter.peakMemoryUsage()
  }

  // TODO: when to cleanup?
  def cleanupResources(): Unit = {
    sorter.cleanupResources()
  }

}
