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
import org.apache.spark.sql.catalyst.vector.{ColumnVector, RowBatch}

/**
  * An expression that is evaluated to the first non-null input.
  *
  * {{{
  *   coalesce(1, 2) => 1
  *   coalesce(null, 1, 2) => 1
  *   coalesce(null, null, 2) => 2
  *   coalesce(null, null, null) => null
  * }}}
  */
case class BatchCoalesce(
    children: Seq[BatchExpression], underlyingExpr: Expression) extends BatchExpression {

  override def eval(input: RowBatch): ColumnVector = null

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val first = children(0)
    val rest = children.drop(1)
    val firstEval = first.genCode(ctx)

    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val tmp = ctx.freshName("tmp")

    val get = ctx.getMethodName(dataType)
    val put = ctx.putMethodName(dataType)

    s"""
      int[] $tmp = new int[${ctx.getBatchCapacity}];
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      ColumnVector ${ev.value} = null;

      ${firstEval.code}
      if (${firstEval.value}.noNulls) {
        ${ev.value} = ${firstEval.value};
      } else {
        ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType)};
        ${ev.value}.noNulls = false;

        // initiate $tmp array to denote rows in needed (set to 1)
        if ($selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $tmp[i] = 1;
          }
        } else {
          Arrays.fill($tmp, 1);
        }

        if (!${firstEval.value}.isRepeating) {
          if ($selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              if ($tmp[i] == 1 && !${firstEval.value}.isNullAt(i)) {
                ${ev.value}.$put(i, ${firstEval.value}.$get(i));
                $tmp[i] = 0;
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              if ($tmp[i] == 1 && !${firstEval.value}.isNullAt(i)) {
                ${ev.value}.$put(i, ${firstEval.value}.$get(i));
                $tmp[i] = 0;
              }
            }
          }
        } else {
          ; // if all tuple evals to null, nothing to do with this cv
        }
      }
    """ +
      rest.map { be =>
      val curEval = be.genCode(ctx)
      s"""
        if (!${ev.value}.noNulls) {
          ${curEval.code}
          if (${curEval.value}.noNulls) {
            ${ev.value}.noNulls = true;
            if (${curEval.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if ($tmp[i] == 1) {
                    ${ev.value}.$put(i, ${curEval.value}.$get(0));
                    $tmp[i] = 0;
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if ($tmp[i] == 1) {
                    ${ev.value}.$put(i, ${curEval.value}.$get(0));
                    $tmp[i] = 0;
                  }
                }
              }
            } else { // no nulls, not repeating
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if ($tmp[i] == 1) {
                    ${ev.value}.$put(i, ${curEval.value}.$get(i));
                    $tmp[i] = 0;
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if ($tmp[i] == 1) {
                    ${ev.value}.$put(i, ${curEval.value}.$get(i));
                    $tmp[i] = 0;
                  }
                }
              }
            }
          } else { // have null in current expression's eval
            if (${curEval.value}.isRepeating) {
              ;
            } else { // have nulls, not repeating
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if ($tmp[i] == 1 && !${curEval.value}.isNullAt(i)) {
                    ${ev.value}.$put(i, ${curEval.value}.$get(i));
                    $tmp[i] = 0;
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if ($tmp[i] == 1 && !${curEval.value}.isNullAt(i)) {
                    ${ev.value}.$put(i, ${curEval.value}.$get(i));
                    $tmp[i] = 0;
                  }
                }
              }
            }
          }
        } else {
          ; // already no nulls in prev result, the current children's eval could be eliminated
        }
      """
      }.mkString("\n") + s"""
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

/**
  * An expression that is evaluated to true if the input is null.
  */
case class BatchIsNull(
    child: BatchExpression, underlyingExpr: Expression) extends UnaryBatchExpression {

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val childEval = child.genCode(ctx)
    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val newSize = ctx.freshName("newSize")
    s"""
      ${childEval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      boolean $selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      int $newSize;
      $newSize = $nu.filterNonNullsOn(${childEval.value}, $selectedInUse, $sel, $batchSize);
      if ($newSize < $batchSize) {
        ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}


/**
  * An expression that is evaluated to true if the input is not null.
  */
case class BatchIsNotNull(
    child: BatchExpression, underlyingExpr: Expression) extends UnaryBatchExpression {

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val childEval = child.genCode(ctx)
    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val newSize = ctx.freshName("newSize")
    s"""
      ${childEval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      boolean $selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      int $newSize;
      $newSize = $nu.filterNullsOn(${childEval.value}, $selectedInUse, $sel, $batchSize);
      if ($newSize < $batchSize) {
        ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}
