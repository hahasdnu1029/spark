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
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types._

abstract class BinaryBatchArithmetic extends BinaryBatchOperator {
  override def dataType: DataType = left.dataType

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {

    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val get = ctx.getMethodName(dataType)
    val put = ctx.putMethodName(dataType)

    val vectorGen: String = if (generateOffHeapColumnVector) {
      s"OffColumnVector ${ev.value} = " +
        s"new OffColumnVector(${eval1.value}.dataType, ${ctx.INPUT_ROWBATCH}.capacity);"
    } else {
      s"OnColumnVector ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType)};"
    }

    val mode: String = if (generateOffHeapColumnVector) "Off" else "On"

    s"""
      ${eval1.code}
      ${eval2.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

      $vectorGen

      ${ev.value}.isRepeating =
        ${eval1.value}.isRepeating && ${eval2.value}.isRepeating ||
        ${eval1.value}.isRepeating && !${eval1.value}.noNulls && ${eval1.value}.isNullAt(0) ||
        ${eval2.value}.isRepeating && !${eval2.value}.noNulls && ${eval2.value}.isNullAt(0);
      $nu.propagateNullsForBinaryExpression$mode(
        ${eval1.value}, ${eval2.value}, ${ev.value},
        $sel, $batchSize, ${ctx.INPUT_ROWBATCH}.selectedInUse);

      /*
       * Disregard nulls for processing. In other words,
       * the arithmetic operation is performed even if one or
       * more inputs are null. This is to improve speed by avoiding
       * conditional checks in the inner loop.
       */
      if (${eval1.value}.isRepeating && ${eval2.value}.isRepeating) {
        ${ev.value}.$put(0, ${eval1.value}.$get(0) $symbol ${eval2.value}.$get(0));
      } else if (${eval1.value}.isRepeating) {
        final ${CodeGenerator.javaType(left.dataType)} leftValue = ${eval1.value}.$get(0);
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ev.value}.$put(i, leftValue $symbol ${eval2.value}.$get(i));
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ev.value}.$put(i, leftValue $symbol ${eval2.value}.$get(i));
          }
        }
      } else if (${eval2.value}.isRepeating) {
        final ${CodeGenerator.javaType(right.dataType)} rightValue = ${eval2.value}.$get(0);
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ev.value}.$put(i, ${eval1.value}.$get(i) $symbol rightValue);
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ev.value}.$put(i, ${eval1.value}.$get(i) $symbol rightValue);
          }
        }
      } else {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ev.value}.$put(i, ${eval1.value}.$get(i) $symbol ${eval2.value}.$get(i));
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ev.value}.$put(i, ${eval1.value}.$get(i) $symbol ${eval2.value}.$get(i));
          }
        }
      }

      /*
       * For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      $nu.setNullDataEntries${CodeGenerator.boxedType(dataType)}$mode(
        ${ev.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $batchSize);

    """
  }
}

case class BatchAdd(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchSubtract(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchMultiply(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchBitwiseAnd(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "&"

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchDivide(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "/"

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {

    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val get = ctx.getMethodName(left.dataType)
    val put = ctx.putMethodName(dataType)
    s"""
      ${eval1.code}
      ${eval2.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

      OnColumnVector ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType)};

      ${ev.value}.isRepeating =
        ${eval1.value}.isRepeating && ${eval2.value}.isRepeating ||
        ${eval1.value}.isRepeating && !${eval1.value}.noNulls && ${eval1.value}.isNullAt(0) ||
        ${eval2.value}.isRepeating && !${eval2.value}.noNulls && ${eval2.value}.isNullAt(0);
      $nu.propagateNullsForBinaryExpressionOn(
        ${eval1.value}, ${eval2.value}, ${ev.value},
        $sel, $batchSize, ${ctx.INPUT_ROWBATCH}.selectedInUse);
      $nu.propagateZeroDenomAsNulls${CodeGenerator.boxedType(dataType)}On(${eval2.value}, ${ev.value},
        $sel, $batchSize, ${ctx.INPUT_ROWBATCH}.selectedInUse);

      /*
       * Disregard nulls for processing. In other words,
       * the arithmetic operation is performed even if one or
       * more inputs are null. This is to improve speed by avoiding
       * conditional checks in the inner loop.
       */
      if (${eval1.value}.isRepeating && ${eval2.value}.isRepeating) {
        ${ev.value}.$put(0, ${eval1.value}.$get(0) $symbol ${eval2.value}.$get(0));
      } else if (${eval1.value}.isRepeating) {
        final ${CodeGenerator.javaType(left.dataType)} leftValue = ${eval1.value}.$get(0);
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ev.value}.$put(i, leftValue $symbol ${eval2.value}.$get(i));
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ev.value}.$put(i, leftValue $symbol ${eval2.value}.$get(i));
          }
        }
      } else if (${eval2.value}.isRepeating) {
        final ${CodeGenerator.javaType(right.dataType)} rightValue = ${eval2.value}.$get(0);
        if (rightValue == 0) {
          // this part is handled in propagateZeroDenomAsNulls
        } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ev.value}.$put(i, ${eval1.value}.$get(i) $symbol rightValue);
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ev.value}.$put(i, ${eval1.value}.$get(i) $symbol rightValue);
          }
        }
      } else {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ev.value}.$put(i, ${eval1.value}.$get(i) $symbol ${eval2.value}.$get(i));
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ev.value}.$put(i, ${eval1.value}.$get(i) $symbol ${eval2.value}.$get(i));
          }
        }
      }

      /* For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      $nu.setNullDataEntries${CodeGenerator.boxedType(dataType)}On(
        ${ev.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $batchSize);

    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchPmod(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "pmod"

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val get = ctx.getMethodName(left.dataType)
    val put = ctx.putMethodName(dataType)

    s"""
      ${eval1.code}
      ${eval2.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      boolean $selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      OnColumnVector ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType)};

      ${ev.value}.isRepeating =
        ${eval1.value}.isRepeating && ${eval2.value}.isRepeating ||
        ${eval1.value}.isRepeating && !${eval1.value}.noNulls && ${eval1.value}.isNullAt(0) ||
        ${eval2.value}.isRepeating && !${eval2.value}.noNulls && ${eval2.value}.isNullAt(0);
      $nu.propagateNullsForBinaryExpressionOn(
        ${eval1.value}, ${eval2.value}, ${ev.value},
        $sel, $batchSize, $selectedInUse);
      $nu.propagateZeroDenomAsNulls${CodeGenerator.boxedType(dataType)}On(${eval2.value}, ${ev.value},
        $sel, $batchSize, $selectedInUse);

      if (${eval1.value}.isRepeating && ${eval2.value}.isRepeating) {
        ${CodeGenerator.javaType(right.dataType)} right = ${eval2.value}.$get(0);
        ${CodeGenerator.javaType(dataType)} r = ${eval1.value}.$get(0) % right;
        if (r < 0) {
          ${ev.value}.$put(0, (r + right) % right);
        } else {
          ${ev.value}.$put(0, r);
        }
      } else if (${eval1.value}.isRepeating) {
        final ${CodeGenerator.javaType(left.dataType)} leftValue = ${eval1.value}.$get(0);
        if ($selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${CodeGenerator.javaType(right.dataType)} right = ${eval2.value}.$get(i);
            ${CodeGenerator.javaType(dataType)} r = leftValue % right;
            if (r < 0) {
              ${ev.value}.$put(i, (r + right) % right);
            } else {
              ${ev.value}.$put(i, r);
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${CodeGenerator.javaType(right.dataType)} right = ${eval2.value}.$get(i);
            ${CodeGenerator.javaType(dataType)} r = leftValue % right;
            if (r < 0) {
              ${ev.value}.$put(i, (r + right) % right);
            } else {
              ${ev.value}.$put(i, r);
            }
          }
        }
      } else if (${eval2.value}.isRepeating) {
        final ${CodeGenerator.javaType(right.dataType)} rightValue = ${eval2.value}.$get(0);
        if (rightValue == 0) {
          // this part is handled in propagateZeroDenomAsNulls
        } else if ($selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${CodeGenerator.javaType(dataType)} r = ${eval1.value}.$get(i) % rightValue;
            if (r < 0) {
              ${ev.value}.$put(i, (r + rightValue) % rightValue);
            } else {
              ${ev.value}.$put(i, r);
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${CodeGenerator.javaType(dataType)} r = ${eval1.value}.$get(i) % rightValue;
            if (r < 0) {
              ${ev.value}.$put(i, (r + rightValue) % rightValue);
            } else {
              ${ev.value}.$put(i, r);
            }
          }
        }
      } else {
        if ($selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${CodeGenerator.javaType(right.dataType)} right = ${eval2.value}.$get(i);
            ${CodeGenerator.javaType(dataType)} r = ${eval1.value}.$get(i) % right;
            if (r < 0) {
              ${ev.value}.$put(i, (r + right) % right);
            } else {
              ${ev.value}.$put(i, r);
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${CodeGenerator.javaType(right.dataType)} right = ${eval2.value}.$get(i);
            ${CodeGenerator.javaType(dataType)} r = ${eval1.value}.$get(i) % right;
            if (r < 0) {
              ${ev.value}.$put(i, (r + right) % right);
            } else {
              ${ev.value}.$put(i, r);
            }
          }
        }
      }
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}
