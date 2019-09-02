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

import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.StructType

case class AssembleToRowBatch(child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputsRowBatches: Boolean = true
  override def canProcessRowBatches: Boolean = false
  override def canProcessRows: Boolean = true

  private def defaultBatchCapacity: Int = getParent()._defaultBatchCapacity

  override def doBatchExecute(): RDD[RowBatch] = {
    child.execute().mapPartitionsInternal { iter =>
      val schema = child.output.map(_.dataType)
      val rb = RowBatch.create(schema.toArray, defaultBatchCapacity)
      val rbCapacity = rb.capacity

      val specificInserter = GenerateRowInserter.generate(output, defaultBatchCapacity)

      new Iterator[RowBatch] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): RowBatch = {
          rb.reset()
          var rowCount: Int = 0
          while (iter.hasNext && rowCount < rbCapacity) {
            specificInserter.insert(iter.next(), rb.columns, rowCount)
            rowCount += 1
          }
          rb.size = rowCount
          if (rowCount < rbCapacity) {
            rb.endOfFile = true
          }
          rb
        }
      }
    }
  }

  override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(getClass.getName)
}

case class DissembleFromRowBatch(child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputsRowBatches: Boolean = false
  override def canProcessUnsafeRows: Boolean = false
  override def canProcessRowBatches: Boolean = true
  override def canProcessRows: Boolean = false
  override def doExecute(): RDD[InternalRow] = {
    child.batchExecute().mapPartitionsInternal { iter =>
      val unsafeProjection = UnsafeProjection.create(StructType.fromAttributes(output))
      iter.map(_.rowIterator().asScala).flatten.map(unsafeProjection)
    }
  }
}
