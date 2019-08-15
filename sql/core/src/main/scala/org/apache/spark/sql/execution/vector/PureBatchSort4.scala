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
import org.apache.spark.sql.catalyst.expressions.vector.{GenerateBatchOrdering, GenerateInterBatchOrdering}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.vector.sort4.ExternalRowBatchSorter4
import org.apache.spark.{InternalAccumulator, TaskContext}

/**
  *
  * @param sortOrder
  * @param global
  * @param child
  * @param testSpillFrequency
  */
case class PureBatchSort4(
                           sortOrder: Seq[SortOrder],
                           global: Boolean,
                           child: SparkPlan,
                           testSpillFrequency: Int = 0) extends UnaryExecNode {

  override def outputsUnsafeRows: Boolean = false

  override def canProcessUnsafeRows: Boolean = false

  override def canProcessSafeRows: Boolean = false

  override def canProcessRowBatches: Boolean = true

  override def outputsRowBatches: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  private val defaultBatchCapacity: Int = sqlContext.conf.vectorizedBatchCapacity

  override protected def doBatchExecute(): RDD[RowBatch] = {
    val childOutput = child.output
    val sortTime = longMetric("sortTime")
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")

    child.batchExecute().mapPartitionsInternal { iter =>
      // RowBatch内部排序器
      val innerBatchComparator = GenerateBatchOrdering.generate(
        sortOrder, childOutput, defaultBatchCapacity)
      // RowBatch之间的排序器
      val interBatchComparator = GenerateInterBatchOrdering.generate(
        sortOrder, childOutput, defaultBatchCapacity)

      val sorter = new ExternalRowBatchSorter4(
        childOutput, defaultBatchCapacity, innerBatchComparator, interBatchComparator)

      if (testSpillFrequency > 0) {
        sorter.setTestSpillFrequency(testSpillFrequency)
      }
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val metrics = TaskContext.get().taskMetrics()
      // sort执行之前spill的数据量
      val spillSizeBefore = metrics.memoryBytesSpilled

      TaskContext.get().setMemoryConsumer(this.getId(), sorter.sorter)
      // 排序逻辑
      val sortedIterator = sorter.sort(iter)
      // spill的数据量
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      //      // 排序所用的总时间
      //      sortTime += sorter.getSortTimeNanos / 1000000
      // 峰值内存的大小
      peakMemory += sorter.peakMemoryUsage
      // 增加执行内存的大小
      metrics.incPeakExecutionMemory(sorter.peakMemoryUsage)
      sortedIterator
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val result = doBatchExecute()
    result.mapPartitionsInternal { iter =>
      iter.map(_.rowIterator().asScala).flatten
    }
  }

  //    throw new UnsupportedOperationException(getClass.getName)
}
