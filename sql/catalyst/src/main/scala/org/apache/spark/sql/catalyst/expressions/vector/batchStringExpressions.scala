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

import java.util.regex.Pattern

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GeneratedBatchExpressionCode}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

abstract class BatchStringPredicate extends BinaryBatchExpression {

  def function: String

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val n = ctx.freshName("n")
    val newSize = ctx.freshName("newSize")
    val sel = ctx.freshName("sel")
    val leftStr = ctx.freshName("leftStr")
    val rightStr = ctx.freshName("rightStr")
    val get = ctx.getMethodName(StringType)

    s"""
      ${eval1.code}
      ${eval2.code}
      int $n = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

      UTF8String $leftStr;
      UTF8String $rightStr;

      // filter rows with NULL on left input
      int $newSize;
      $newSize = $nu.filterNullsOn(${eval1.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
      if ($newSize < $n) {
        $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      $newSize = $nu.filterNullsOn(${eval2.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
      if ($newSize < $n) {
        $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      // All rows with nulls has been filtered out, so just do normal filter for no-null case
      if ($n != 0 && ${eval1.value}.isRepeating && ${eval2.value}.isRepeating) {
        $leftStr = ${eval1.value}.$get(0);
        $rightStr = ${eval2.value}.$get(0);
        if (!$leftStr.$function($rightStr)) {
          ${ctx.INPUT_ROWBATCH}.size = 0;
        }
      } else if (${eval1.value}.isRepeating) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          $newSize = 0;
          $leftStr = ${eval1.value}.$get(0);
          for (int j = 0; j < $n; j ++) {
            int i = $sel[j];
            $rightStr = ${eval2.value}.$get(i);
            if ($leftStr.$function($rightStr)) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          $leftStr = ${eval1.value}.$get(0);
          for (int i = 0; i < $n; i ++) {
            $rightStr = ${eval2.value}.$get(i);
            if ($leftStr.$function($rightStr)) {
              $sel[$newSize ++] = i;
            }
          }
          if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }
        }
      } else if (${eval2.value}.isRepeating) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          $newSize = 0;
          $rightStr = ${eval2.value}.$get(0);
          for (int j = 0; j < $n; j ++) {
            int i = $sel[j];
            $leftStr = ${eval1.value}.$get(i);
            if ($leftStr.$function($rightStr)) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          $rightStr = ${eval2.value}.$get(0);
          for (int i = 0; i < $n; i ++) {
            $leftStr = ${eval1.value}.$get(i);
            if ($leftStr.$function($rightStr)) {
              $sel[$newSize ++] = i;
            }
          }
          if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }
        }
      } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
        $newSize = 0;
        for (int j = 0; j < $n; j ++) {
          int i = $sel[j];
          $leftStr = ${eval1.value}.$get(i);
          $rightStr = ${eval2.value}.$get(i);
          if ($leftStr.$function($rightStr)) {
            $sel[$newSize ++] = i;
          }
        }
        ${ctx.INPUT_ROWBATCH}.size = $newSize;
      } else {
        $newSize = 0;
        for (int i = 0; i < $n; i ++) {
          $leftStr = ${eval1.value}.$get(i);
          $rightStr = ${eval2.value}.$get(i);
          if ($leftStr.$function($rightStr)) {
            $sel[$newSize ++] = i;
          }
        }
        if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
          ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
        }
      }
    """
  }
}

case class BatchStartsWith(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BatchStringPredicate {
  override def function: String = "startsWith"

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchEndsWith(
  left: BatchExpression,
  right: BatchExpression,
  underlyingExpr: Expression) extends BatchStringPredicate {
  override def function: String = "endsWith"
  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchContains(
  left: BatchExpression,
  right: BatchExpression,
  underlyingExpr: Expression) extends BatchStringPredicate {
  override def function: String = "contains"
  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchLike(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BatchStringPredicate {

  override def function: String = "like"

  def escape(v: String): String = StringUtils.escapeLikeRegex(v)

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val patternClass = classOf[Pattern].getName
    val pattern = ctx.freshName("pattern")
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    if (right.foldable) {
      val rVal = right.underlyingExpr.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(escape(rVal.asInstanceOf[UTF8String].toString()))
        ctx.addMutableState(patternClass, pattern, v=>s"""$v = ${patternClass}.compile("$regexStr");""")

        val eval = left.genCode(ctx)
        val n = ctx.freshName("n")
        val newSize = ctx.freshName("newSize")
        val sel = ctx.freshName("sel")
        val str = ctx.freshName("str")
        val get = ctx.getMethodName(StringType)

        s"""
          ${eval.code}
          int $n = ${ctx.INPUT_ROWBATCH}.size;
          int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

          UTF8String $str;

          // filter rows with NULL on left input
          int $newSize;
          $newSize = $nu.filterNullsOn(${eval.value},
            ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
          if ($newSize < $n) {
            $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }

          // All rows with nulls has been filtered out, so just do normal filter for no-null case
          if ($n != 0 && ${eval.value}.isRepeating) {
            $str = ${eval.value}.$get(0);
            if (!$pattern.matcher($str.toString()).matches()) {
              ${ctx.INPUT_ROWBATCH}.size = 0;
            }
          } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              $str = ${eval.value}.$get(i);
              if ($pattern.matcher($str.toString()).matches()) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              $str = ${eval.value}.$get(i);
              if ($pattern.matcher($str.toString()).matches()) {
                $sel[$newSize ++] = i;
              }
            }
            if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
              ${ctx.INPUT_ROWBATCH}.size = $newSize;
              ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
            }
          }
        """
      } else {
        "Not Implemented yet"
      }
    } else {
      "Not Implemented yet"
    }
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

// TODO: Only deals with Strings construct of characters using exact one byte to represent
case class BatchSubstring(
    str: BatchExpression,
    pos: BatchExpression,
    len: BatchExpression,
    underlyingExpr: Expression) extends TernaryBatchExpression {

  override def dataType: DataType = str.dataType

  override def children: Seq[BatchExpression] = str :: pos :: len :: Nil

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    if (pos.foldable && len.foldable) {
      val position = pos.underlyingExpr.eval()
      val length = len.underlyingExpr.eval()

      val eval = str.genCode(ctx)
      val n = ctx.freshName("n")
      val sel = ctx.freshName("sel")

      if (position != null && length != null) {
        s"""
          ${eval.code}
          int $n = ${ctx.INPUT_ROWBATCH}.size;
          int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
          OnColumnVector ${ev.value} = new OnColumnVector(${eval.value});

          if (${ev.value}.noNulls) {
            if (${ev.value}.isRepeating) {
              if ($position > ${ev.value}.lengths[0]) {
                ${ev.value}.isNull[0] = true;
                ${ev.value}.noNulls = false;
              } else {
                ${ev.value}.starts[0] += $position;
                int remain = ${ev.value}.lengths[0] - $position;
                ${ev.value}.lengths[0] = (remain < $length) ? remain : $length;
              }
            } else {
              if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
                for (int j = 0; j < $n; j ++) {
                  int i = $sel[j];
                  if ($position > ${ev.value}.lengths[i]) {
                    ${ev.value}.isNull[i] = true;
                    ${ev.value}.noNulls = false;
                  } else {
                    ${ev.value}.starts[i] += $position;
                    int remain = ${ev.value}.lengths[i] - $position;
                    ${ev.value}.lengths[i] = (remain < $length) ? remain : $length;
                  }
                }
              } else {
                for (int i = 0; i < $n; i ++) {
                  if ($position > ${ev.value}.lengths[i]) {
                    ${ev.value}.isNull[i] = true;
                    ${ev.value}.noNulls = false;
                  } else {
                    ${ev.value}.starts[i] += $position;
                    int remain = ${ev.value}.lengths[i] - $position;
                    ${ev.value}.lengths[i] = (remain < $length) ? remain : $length;
                  }
                }
              }
            }
          } else {
            if (${ev.value}.isRepeating) {
              // noNulls is false && isRepeating, nothing to do here
            } else {
              if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
                for (int j = 0; j < $n; j ++) {
                  int i = $sel[j];
                  if (!${ev.value}.isNullAt(i)) {
                    if ($position > ${ev.value}.lengths[i]) {
                      ${ev.value}.isNull[i] = true;
                    } else {
                      ${ev.value}.starts[i] += $position;
                      int remain = ${ev.value}.lengths[i] - $position;
                      ${ev.value}.lengths[i] = (remain < $length) ? remain : $length;
                    }
                  }
                }
              } else {
                for (int i = 0; i < $n; i ++) {
                  if (!${ev.value}.isNullAt(i)) {
                    if ($position > ${ev.value}.lengths[i]) {
                      ${ev.value}.isNull[i] = true;
                    } else {
                      ${ev.value}.starts[i] += $position;
                      int remain = ${ev.value}.lengths[i] - $position;
                      ${ev.value}.lengths[i] = (remain < $length) ? remain : $length;
                    }
                  }
                }
              }
            }
          }
        """
      } else {
        "Not Implemented yet"
      }
    } else {
      "Not Implemented yet"
    }
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}
