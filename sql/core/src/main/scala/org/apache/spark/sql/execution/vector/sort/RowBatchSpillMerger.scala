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

package org.apache.spark.sql.execution.vector.sort

import java.io.IOException
import java.util.{Comparator, PriorityQueue}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.{GenerateBatchCopier, InterBatchOrdering}
import org.apache.spark.sql.catalyst.vector.RowBatch

case class RowBatchSpillMerger(
    val interBatchOrdering: InterBatchOrdering,
    val numSpills: Int,
    val schema: Seq[Attribute],
    val defaultCapacity: Int) {

  // we assume row batches get from the iterator are already clustered, therefore we just need to
  // iterate each batch sequentially
  val comparator: Comparator[RowBatchSorterIterator] = new Comparator[RowBatchSorterIterator] {
    override def compare(
        i1: RowBatchSorterIterator,
        i2: RowBatchSorterIterator): Int = {
      interBatchOrdering.compare(
        i1.currentBatch, i1.currentBatch.rowIdx,
        i2.currentBatch, i2.currentBatch.rowIdx)
    }
  }

  val priorityQueue = new PriorityQueue[RowBatchSorterIterator](numSpills, comparator)

  val batchCopier = GenerateBatchCopier.generate(schema, defaultCapacity)

  def addSpillIfNotEmpty(spillReader: RowBatchSorterIterator): Unit = {
    if (spillReader.hasNext()) {
      spillReader.loadNext()
      spillReader.currentBatch.rowIdx = 0
      priorityQueue.add(spillReader)
    }
  }

  def getSortedIterator(): RowBatchSorterIterator = {
    new RowBatchSorterIterator {

      val rb: RowBatch = RowBatch.create(schema.map(_.dataType).toArray, defaultCapacity)

      override def hasNext(): Boolean = !priorityQueue.isEmpty

      @throws[IOException]
      override def loadNext(): Unit = {
        rb.reset()
        var rowIdx: Int = 0

        while (rowIdx < defaultCapacity && !priorityQueue.isEmpty) {

          val nextLineSource = priorityQueue.remove()
          val nextLineBatch = nextLineSource.currentBatch

          batchCopier.copy(nextLineBatch, nextLineBatch.rowIdx, rb, rowIdx)

          nextLineBatch.rowIdx += 1
          if (nextLineBatch.rowIdx < nextLineBatch.size) {
            priorityQueue.add(nextLineSource)
          } else if (nextLineSource.hasNext()) {
            nextLineSource.loadNext()
            nextLineSource.currentBatch.rowIdx = 0
            priorityQueue.add(nextLineSource)
          }

          rowIdx += 1
        }
      }

      override def currentBatch: RowBatch = rb
    }
  }
}
