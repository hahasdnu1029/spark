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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, GeneratedBatchExpressionCode}
import org.apache.spark.sql.types._
import org.apache.zookeeper.KeeperException.UnimplementedException

case class BatchCast(
    child: BatchExpression,
    underlyingExpr: Expression) extends UnaryBatchExpression {

  override protected def doGenCode(
                                  ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")
    val eval = child.genCode(ctx)

    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val get = ctx.getMethodName(child.dataType)
    val put = ctx.putMethodName(dataType)

    def cvCopy(castSingle: String): String = {
      val cal = s"""
        ${ev.value} = ${ctx.newVector(s"${eval.value}.getCapacity()", dataType)};
        ${ev.value}.noNulls = ${eval.value}.noNulls;
        ColumnVector.copyNulls(${eval.value}, 0, ${ev.value}, 0, ${eval.value}.getCapacity());
        ${ev.value}.isRepeating = ${eval.value}.isRepeating;
        if (${ev.value}.isRepeating && ${ev.value}.noNulls) {
          int i = 0;
          $castSingle
        } else if (${ev.value}.isRepeating) {
          ; // we've already copied isNull array
        } else if (${ev.value}.noNulls) {
          if ($selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              $castSingle
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              $castSingle
            }
          }
        } else {
          if ($selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              if (!${ev.value}.isNullAt(i)) {
                $castSingle
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              if (!${ev.value}.isNullAt(i)) {
                $castSingle
              }
            }
          }
        }
      """
      val setNullEntries = if (dataType.isInstanceOf[NumericType]) {
        s"""
         /* For the case when the output can have null values, follow
          * the convention that the data values must be 1 for long and
          * NaN for double. This is to prevent possible later zero-divide errors
          * in complex arithmetic expressions like col2 / (col1 - 1)
          * in the case when some col1 entries are null.
          */
         $nu.setNullDataEntries${CodeGenerator.boxedType(dataType)}On(
           ${ev.value}, $selectedInUse, $sel, $batchSize);
         """
      } else ""
      cal + setNullEntries
    }

    val castToIntCode = child.dataType match {
      case StringType =>
        s"""
          try {
            ${ev.value}.$put(i, Integer.valueOf(${eval.value}.getString(i).toString());
          } catch (java.lang.NumberFormatException e) {
            ${ev.value}.putNull(i);
          }
        """
      case x: NumericType => s"${ev.value}.$put(i, (int) ${eval.value}.$get(i));"
    }

    val castToLongCode = child.dataType match {
      case StringType =>
        s"""
          try {
            ${ev.value}.$put(i, Long.valueOf(${eval.value}.getString(i).toString());
          } catch (java.lang.NumberFormatException e) {
            ${ev.value}.putNull(i);
          }
        """
      case x: NumericType => s"${ev.value}.$put(i, (long) ${eval.value}.$get(i));"
    }

    val castToDoubleCode = child.dataType match {
      case StringType =>
        s"""
          try {
            ${ev.value}.$put(i, Double.valueOf(${eval.value}.getString(i).toString());
          } catch (java.lang.NumberFormatException e) {
            ${ev.value}.putNull(i);
          }
        """
      case x: NumericType => s"${ev.value}.$put(i, (double) ${eval.value}.$get(i));"
    }

    val castToStringCode = child.dataType match {
      case _ => s"${ev.value}.putString(i, String.valueOf(${eval.value}.$get(i)));"
    }

    val castCode = dataType match {
      case _ if child.dataType == dataType => s"${ev.value} = ${eval.value};"
      case IntegerType => cvCopy(castToIntCode)
      case LongType => cvCopy(castToLongCode)
      case DoubleType => cvCopy(castToDoubleCode)
      case StringType => cvCopy(castToStringCode)
      case _ => throw new UnimplementedException
    }

    s"""
      ${eval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      boolean $selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      ColumnVector ${ev.value} = null;
      $castCode
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}
