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

  // 是否输出UnsafeRow？===》false
  override def outputsUnsafeRows: Boolean = false

  // 是否能够处理UnsafeRow？====》false
  override def canProcessUnsafeRows: Boolean = false

  // 是否能处理SafeRow？=====》false
  override def canProcessSafeRows: Boolean = false

  // 是否能处理RowBatch？=====》true
  override def canProcessRowBatches: Boolean = true

  // 输出RowBatch====》true
  override def outputsRowBatches: Boolean = true

  // 输出属性(schema)和子类一样
  override def output: Seq[Attribute] = child.output

  // outputOrdering=sortOder[这个表达式没有绑定schema]
  override def outputOrdering: Seq[SortOrder] = sortOrder

  // 是否进行Distribution
  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  // metrics信息(sortTime,peakMemory,spillSize)
  override val metrics = Map(
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  // 默认的Batch容量
  private val defaultBatchCapacity: Int = sqlContext.conf.vectorizedBatchCapacity

  override protected def doBatchExecute(): RDD[RowBatch] = {
    // 为了适应Sort的下一个算子是AssembleRowBathc(1024===>配置的)
    setDefaultBatchCapacity(sqlContext.conf.vectorizedBatchCapacity)
    val childOutput = child.output
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")

    child.batchExecute().mapPartitionsInternal { iter =>
      // RowBatch内部排序器
      val innerBatchComparator = GenerateBatchOrdering.generate(
        sortOrder, childOutput, defaultBatchCapacity)
      // RowBatch之间的排序器
      val interBatchComparator = GenerateInterBatchOrdering.generate(
        sortOrder, childOutput, defaultBatchCapacity)

      // 创建ExternalRowBatchSorter4
      val sorter = new ExternalRowBatchSorter4(
        childOutput, defaultBatchCapacity, innerBatchComparator, interBatchComparator)

      println("=======BatchSort============")
      if (testSpillFrequency > 0) {
        sorter.setTestSpillFrequency(testSpillFrequency)
      }
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val metrics = TaskContext.get().taskMetrics()
      // sort执行之前spill的数据量
      val spillSizeBefore = metrics.memoryBytesSpilled

      // 这里指定了task memory consumer
      TaskContext.get().setMemoryConsumer(this.getId(), sorter.sorter)
      // 排序逻辑,返回的Iterator[RowBatch]
      val sortedIterator = sorter.sort(iter)
      // spill的数据量
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      // 峰值内存的大小
      peakMemory += sorter.peakMemoryUsage
      // 增加执行内存的大小
      metrics.incPeakExecutionMemory(sorter.peakMemoryUsage)
      sortedIterator
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(getClass.getName)
  }
}
