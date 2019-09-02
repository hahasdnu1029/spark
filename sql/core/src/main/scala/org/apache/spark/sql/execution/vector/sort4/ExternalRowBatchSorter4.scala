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


  // 创建ExternalBatchSorter4，继承自MemoryConsumer，制定了使用OFF_Heap Memory
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


  var numBatchesInserted = 0

  val innerBatchCmp = new IntComparator() {
    def compare(i1: Int, i2: Int): Int = innerBatchComparator.compare(i1, i2)
  }

  // 插入一个RowBatch
  def insertBatch(rb: RowBatch): Unit = {

    innerBatchComparator.reset(rb)
    // 先对这个RowBatch进行内部排序
    rb.sort(innerBatchCmp)
    //ExternalBatchSorter4(MemoryConsumer)内存插入
    sorter.insertBatch(rb)
    numBatchesInserted += 1
    println("=========insertBatch之前==========")
    if (testSpillFrequency > 0 && (numBatchesInserted % testSpillFrequency) == 0) {
      // 每次插入完检测是否需要spill，需要则进行spill
      println("=========进入spill============")
      sorter.spill()
    }
  }

  def sort(iter: Iterator[RowBatch]): Iterator[RowBatch] = {
    // 一条一条插入，传入的是RowBatch
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
