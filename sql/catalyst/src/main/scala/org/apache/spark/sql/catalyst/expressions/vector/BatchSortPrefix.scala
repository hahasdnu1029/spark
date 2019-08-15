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

package org.apache.spark.sql.catalyst.expressions.vector

import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, SortDirection}
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedBatchExpressionCode, CodegenContext}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.DoublePrefixComparator

case class BatchSortOrder(
    child: BatchExpression,
    direction: SortDirection,
    underlyingExpr: Expression) extends UnaryBatchExpression {

  override protected def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  override def foldable: Boolean = false
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable

  def isAscending: Boolean = direction == Ascending

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchSortPrefix(
    child: BatchSortOrder,
    underlyingExpr: Expression) extends UnaryBatchExpression {

  override protected def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = child.child.genCode(ctx)

    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val childStr = ctx.freshName("childStr")
    val get = ctx.getMethodName(child.dataType)
    val put = ctx.putMethodName(LongType)

    val DoublePrefixCmp = classOf[DoublePrefixComparator].getName
    val doubleMin = DoublePrefixComparator.computePrefix(Double.NegativeInfinity)

    val prefixGenCode = child.dataType match {
      case StringType =>
        s"""
          UTF8String $childStr;
          if (${eval.value}.noNulls) {
            if (${eval.value}.isRepeating) {
              $childStr = ${eval.value}.$get(0);
              ${ev.value}.$put(0, $childStr.getPrefix());
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                $childStr = ${eval.value}.$get(i);
                ${ev.value}.$put(i, $childStr.getPrefix());
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                $childStr = ${eval.value}.$get(i);
                ${ev.value}.$put(i, $childStr.getPrefix());
              }
            }
          } else {
            if (${eval.value}.isRepeating) {
              ${ev.value}.$put(0, 0L);
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                if (${eval.value}.isNullAt(i)) {
                  ${ev.value}.$put(i, 0L);
                } else {
                  $childStr = ${eval.value}.$get(i);
                  ${ev.value}.$put(i, $childStr.getPrefix());
                }
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                if (${eval.value}.isNullAt(i)) {
                  ${ev.value}.$put(i, 0L);
                } else {
                  $childStr = ${eval.value}.$get(i);
                  ${ev.value}.$put(i, $childStr.getPrefix());
                }
              }
            }
          }
        """
      case _: IntegralType =>
        s"""
          if (${eval.value}.noNulls) {
            if (${eval.value}.isRepeating) {
              ${ev.value}.$put(0, (long) ${eval.value}.$get(0));
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                ${ev.value}.$put(i, (long) ${eval.value}.$get(i));
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                ${ev.value}.$put(i, (long) ${eval.value}.$get(i));
              }
            }
          } else {
            if (${eval.value}.isRepeating) {
              ${ev.value}.$put(0, ${Long.MinValue}L);
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                if (${eval.value}.isNullAt(i)) {
                  ${ev.value}.$put(i, ${Long.MinValue}L);
                } else {
                  ${ev.value}.$put(i, (long) ${eval.value}.$get(i));
                }
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                if (${eval.value}.isNullAt(i)) {
                  ${ev.value}.$put(i, ${Long.MinValue}L);
                } else {
                  ${ev.value}.$put(i, (long) ${eval.value}.$get(i));
                }
              }
            }
          }
        """
      case FloatType | DoubleType =>
        s"""
          if (${eval.value}.noNulls) {
            if (${eval.value}.isRepeating) {
              ${ev.value}.$put(0, $DoublePrefixCmp.computePrefix((double) ${eval.value}.$get(0)));
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                ${ev.value}.$put(i, $DoublePrefixCmp.computePrefix((double) ${eval.value}.$get(i)));
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                ${ev.value}.$put(i, $DoublePrefixCmp.computePrefix((double) ${eval.value}.$get(i)));
              }
            }
          } else {
            if (${eval.value}.isRepeating) {
              ${ev.value}.$put(0, ${doubleMin}L);
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                if (${eval.value}.isNullAt(i)) {
                  ${ev.value}.$put(i, ${doubleMin}L);
                } else {
                  ${ev.value}.$put(i,
                    $DoublePrefixCmp.computePrefix((double) ${eval.value}.$get(i)));
                }
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                if (${eval.value}.isNullAt(i)) {
                  ${ev.value}.$put(i, ${doubleMin}L);
                } else {
                  ${ev.value}.$put(i,
                    $DoublePrefixCmp.computePrefix((double) ${eval.value}.$get(i)));
                }
              }
            }
          }
        """
      case _ => "Not implemented yet"
    }

    s"""
      ${eval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      boolean $selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      OnColumnVector ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", LongType)};
      ${ev.value}.noNulls = true;
      $prefixGenCode
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}
