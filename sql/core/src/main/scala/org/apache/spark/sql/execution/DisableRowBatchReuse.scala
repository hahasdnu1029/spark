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

package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.vector._

//private[sql] object DisableRowBatchReuse extends Rule[SparkPlan] {
//
//  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
//    case operator @ PureBatchSort(_, _, _, _) =>
//      operator.child.shouldReuseRowBatch = false
//      operator
//    case operator @ PureBatchSort0(_, _, _, _) =>
//      operator.child.shouldReuseRowBatch = false
//      operator
//  }
//}

//private[sql] case class ReplaceFilter(sqlContext: SQLContext) extends Rule[SparkPlan] {
//
//  def vectorizeEnabled: Boolean = sqlContext.conf.vectorizedExecutionEnabled
//
//  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
//    case operator @ Filter(condition, child) if vectorizeEnabled && child.outputsRowBatches =>
//      BatchFilter(condition, child)
//  }
//}

private[sql] case class PropagateCapacity(sqlContext: SQLContext) extends Rule[SparkPlan] {
  def vectorizeEnabled: Boolean = sqlContext.conf.vectorizedExecutionEnabled
  def sortBatchCapacity: Int = sqlContext.conf.vectorizedSortBatchCapacity

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
//    case operator @ PureBatchSort2(_, _, _, _) =>
//      operator.setDefaultBatchCapacity(sortBatchCapacity)
//      operator
//    case operator @ BatchSortMergeJoin(_, _, _: PureBatchSort2, right) =>
//      right.setDefaultBatchCapacity(sortBatchCapacity)
//      operator.setDefaultBatchCapacity(sortBatchCapacity)
//      operator
//    case operator @ BatchSortMergeJoin(_, _, left, _: PureBatchSort2) =>
//      left.setDefaultBatchCapacity(sortBatchCapacity)
//      operator.setDefaultBatchCapacity(sortBatchCapacity)
//      operator
//    case operator @ BatchSortMergeOuterJoin(_, _, _, _, _: PureBatchSort2, right) =>
//      right.setDefaultBatchCapacity(sortBatchCapacity)
//      operator.setDefaultBatchCapacity(sortBatchCapacity)
//      operator
//    case operator @ BatchSortMergeOuterJoin(_, _, _, _, left, _: PureBatchSort2) =>
//      left.setDefaultBatchCapacity(sortBatchCapacity)
//      operator.setDefaultBatchCapacity(sortBatchCapacity)
//      operator
    case operator: UnaryExecNode =>
      operator.setDefaultBatchCapacity(operator.child.resultCapacity)
      operator
  }
}