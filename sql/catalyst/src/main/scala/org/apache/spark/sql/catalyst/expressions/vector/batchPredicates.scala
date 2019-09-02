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

import org.apache.spark.sql.catalyst.expressions.{Expression, In, InSet}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, GeneratedBatchExpressionCode}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types._

// scalastyle:off
abstract class BinaryBatchComparison extends BinaryBatchOperator {

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val n = ctx.freshName("n")
    val newSize = ctx.freshName("newSize")
    val sel = ctx.freshName("sel")
    val get = ctx.getMethodName(left.dataType)

    if (CodeGenerator.isPrimitiveType(left.dataType)
        && left.dataType != BooleanType
        && left.dataType != FloatType
        && left.dataType != DoubleType) {
      s"""
        ${eval1.code}
        ${eval2.code}
        int $n = ${ctx.INPUT_ROWBATCH}.size;
        int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

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
          if (!(${eval1.value}.$get(0) $symbol ${eval2.value}.$get(0))) {
            ${ctx.INPUT_ROWBATCH}.size = 0;
          }
        } else if (${eval1.value}.isRepeating) {
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              if (${eval1.value}.$get(0) $symbol ${eval2.value}.$get(i)) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              if (${eval1.value}.$get(0) $symbol ${eval2.value}.$get(i)) {
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
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              if (${eval1.value}.$get(i) $symbol ${eval2.value}.$get(0)) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              if (${eval1.value}.$get(i) $symbol ${eval2.value}.$get(0)) {
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
            if (${eval1.value}.$get(i) $symbol ${eval2.value}.$get(i)) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if (${eval1.value}.$get(i) $symbol ${eval2.value}.$get(i)) {
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
      s"""
        ${eval1.code}
        ${eval2.code}
        int $n = ${ctx.INPUT_ROWBATCH}.size;
        int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

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
          if (!(${ctx.genComp(left.dataType, s"${eval1.value}.$get(0)", s"${eval2.value}.$get(0)")} $symbol 0)) {
            ${ctx.INPUT_ROWBATCH}.size = 0;
          }
        } else if (${eval1.value}.isRepeating) {
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              if (${ctx.genComp(left.dataType, s"${eval1.value}.$get(0)", s"${eval2.value}.$get(i)")} $symbol 0) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              if (${ctx.genComp(left.dataType, s"${eval1.value}.$get(0)", s"${eval2.value}.$get(i)")} $symbol 0) {
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
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              if (${ctx.genComp(left.dataType, s"${eval1.value}.$get(i)", s"${eval2.value}.$get(0)")} $symbol 0) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              if (${ctx.genComp(left.dataType, s"${eval1.value}.$get(i)", s"${eval2.value}.$get(0)")} $symbol 0) {
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
            if (${ctx.genComp(left.dataType, s"${eval1.value}.$get(i)", s"${eval2.value}.$get(i)")} $symbol 0) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if (${ctx.genComp(left.dataType, s"${eval1.value}.$get(i)", s"${eval2.value}.$get(i)")} $symbol 0) {
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
}

case class BatchEqualTo(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {

  override def inputType: AbstractDataType = AnyDataType

  override def symbol: String = "="

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val n = ctx.freshName("n")
    val newSize = ctx.freshName("newSize")
    val sel = ctx.freshName("sel")
    val get = ctx.getMethodName(left.dataType)

    if (left.dataType.isInstanceOf[StringType]) {
      val leftStr = ctx.freshName("leftStr")
      val rightStr = ctx.freshName("rightStr")

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
          if (!$leftStr.equals($rightStr)) {
            ${ctx.INPUT_ROWBATCH}.size = 0;
          }
        } else if (${eval1.value}.isRepeating) {
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            $leftStr = ${eval1.value}.$get(0);
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              $rightStr = ${eval2.value}.$get(i);
              if ($leftStr.equals($rightStr)) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            $leftStr = ${eval1.value}.$get(0);
            for (int i = 0; i < $n; i ++) {
              $rightStr = ${eval2.value}.$get(i);
              if ($leftStr.equals($rightStr)) {
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
              if ($leftStr.equals($rightStr)) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            $rightStr = ${eval2.value}.$get(0);
            for (int i = 0; i < $n; i ++) {
              $leftStr = ${eval1.value}.$get(i);
              if ($leftStr.equals($rightStr)) {
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
            if ($leftStr.equals($rightStr)) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            $leftStr = ${eval1.value}.$get(i);
            $rightStr = ${eval2.value}.$get(i);
            if ($leftStr.equals($rightStr)) {
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
      s"""
        ${eval1.code}
        ${eval2.code}
        int $n = ${ctx.INPUT_ROWBATCH}.size;
        int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

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
          if (!(${ctx.genEqual(left.dataType, s"${eval1.value}.$get(0)", s"${eval2.value}.$get(0)")})) {
            ${ctx.INPUT_ROWBATCH}.size = 0;
          }
        } else if (${eval1.value}.isRepeating) {
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              if (${ctx.genEqual(left.dataType, s"${eval1.value}.$get(0)", s"${eval2.value}.$get(i)")}) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              if (${ctx.genEqual(left.dataType, s"${eval1.value}.$get(0)", s"${eval2.value}.$get(i)")}) {
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
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              if (${ctx.genEqual(left.dataType, s"${eval1.value}.$get(i)", s"${eval2.value}.$get(0)")}) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              if (${ctx.genEqual(left.dataType, s"${eval1.value}.$get(i)", s"${eval2.value}.$get(0)")}) {
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
            if (${ctx.genEqual(left.dataType, s"${eval1.value}.$get(i)", s"${eval2.value}.$get(i)")}) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if (${ctx.genEqual(left.dataType, s"${eval1.value}.$get(i)", s"${eval2.value}.$get(i)")}) {
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

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchLessThan(
  left: BatchExpression,
  right: BatchExpression,
  underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = TypeCollection.Ordered
  override def symbol: String = "<"

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchLessThanOrEqual(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = TypeCollection.Ordered
  override def symbol: String = "<="

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchGreaterThan(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = TypeCollection.Ordered
  override def symbol: String = ">"

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchGreaterThanOrEqual(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = TypeCollection.Ordered
  override def symbol: String = ">="

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchAnd(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = BooleanType
  override def symbol: String = "&&"

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    eval1.code + eval2.code
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchIn(
    child: BatchExpression,
    list: Seq[Expression],
    underlyingExpr: Expression) extends UnaryBatchExpression {

  assert(list.forall(_.foldable), "only support all literals curently")
  // TODO support null in list

  val cType = child.dataType

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val seqName = classOf[Seq[Any]].getName
    val InName = classOf[In].getName

    ctx.references += underlyingExpr
    val seqTerm = ctx.freshName("seq")

    val n = ctx.freshName("n")
    val sel = ctx.freshName("sel")
    val newSize = ctx.freshName("newSize")
    val get = ctx.getMethodName(cType)

    val str = ctx.freshName("str")
    ctx.addMutableState(seqName, seqTerm,
      v=>s"$v = (($InName)expressions[${ctx.references.size - 1}]).getList();")

    val eval = child.genCode(ctx)
    cType match {
      case StringType =>
        s"""
          ${eval.code}
          int $n = ${ctx.INPUT_ROWBATCH}.size;
          int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

          UTF8String $str;

          int $newSize;
          $newSize = $nu.filterNullsOn(${eval.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
          if ($newSize < $n) {
            $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }

          if ($n != 0 && ${eval.value}.isRepeating) {
            $str = ${eval.value}.$get(0);
            if (!$seqTerm.contains($str)) {
              ${ctx.INPUT_ROWBATCH}.size = 0;
            }
          } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              $str = ${eval.value}.$get(i);
              if ($seqTerm.contains($str)) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              $str = ${eval.value}.$get(i);
              if ($seqTerm.contains($str)) {
                $sel[$newSize ++] = i;
              }
            }
            if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
              ${ctx.INPUT_ROWBATCH}.size = $newSize;
              ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
            }
          }
        """
      case _ =>
        s"""
          ${eval.code}
          int $n = ${ctx.INPUT_ROWBATCH}.size;
          int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

          int $newSize;
          $newSize = $nu.filterNullsOn(${eval.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
          if ($newSize < $n) {
            $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }

          if ($n != 0 && ${eval.value}.isRepeating) {
            if (!$seqTerm.contains(${eval.value}.$get(0))) {
              ${ctx.INPUT_ROWBATCH}.size = 0;
            }
          } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              if ($seqTerm.contains(${eval.value}.$get(i))) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              if ($seqTerm.contains(${eval.value}.$get(i))) {
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

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchInSet(
    child: BatchExpression,
    hset: Set[Any],
    underlyingExpr: Expression) extends UnaryBatchExpression {

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val setName = classOf[Set[Any]].getName
    val InSetName = classOf[InSet].getName

    ctx.references += underlyingExpr
    val hsetTerm = ctx.freshName("hset")
    val hasNullTerm = ctx.freshName("hasNull")

    val n = ctx.freshName("n")
    val sel = ctx.freshName("sel")
    val newSize = ctx.freshName("newSize")
    val get = ctx.getMethodName(child.dataType)

    val str = ctx.freshName("str")

    ctx.addMutableState(setName, hsetTerm,
      v=>s"$v = (($InSetName)expressions[${ctx.references.size - 1}]).getHSet();")
    ctx.addMutableState("boolean", hasNullTerm, v=>s"$v = $hsetTerm.contains(null);")

    val eval = child.genCode(ctx)
    child.dataType match {
      case StringType =>
        s"""
          ${eval.code}
          int $n = ${ctx.INPUT_ROWBATCH}.size;
          int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

          UTF8String $str;

          int $newSize;
          $newSize = $nu.filterNullsOn(${eval.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
          if ($newSize < $n) {
            $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }

          if ($n != 0 && ${eval.value}.isRepeating) {
            $str = ${eval.value}.$get(0);
            if (!$hsetTerm.contains($str)) {
              ${ctx.INPUT_ROWBATCH}.size = 0;
            }
          } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              $str = ${eval.value}.$get(i);
              if ($hsetTerm.contains($str)) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              $str = ${eval.value}.$get(i);
              if ($hsetTerm.contains($str)) {
                $sel[$newSize ++] = i;
              }
            }
            if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
              ${ctx.INPUT_ROWBATCH}.size = $newSize;
              ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
            }
          }
        """
      case _ =>
        s"""
          ${eval.code}
          int $n = ${ctx.INPUT_ROWBATCH}.size;
          int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

          int $newSize;
          $newSize = $nu.filterNullsOn(${eval.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
          if ($newSize < $n) {
            $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }

          if ($n != 0 && ${eval.value}.isRepeating) {
            if (!$hsetTerm.contains(${eval.value}.$get(0))) {
              ${ctx.INPUT_ROWBATCH}.size = 0;
            }
          } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            $newSize = 0;
            for (int j = 0; j < $n; j ++) {
              int i = $sel[j];
              if ($hsetTerm.contains(${eval.value}.$get(i))) {
                $sel[$newSize ++] = i;
              }
            }
            ${ctx.INPUT_ROWBATCH}.size = $newSize;
          } else {
            $newSize = 0;
            for (int i = 0; i < $n; i ++) {
              if ($hsetTerm.contains(${eval.value}.$get(i))) {
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

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchOr(
  left: BatchExpression,
  right: BatchExpression,
  underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = BooleanType
  override def symbol: String = "&&"

  // TODO: reuse int array?
  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val initialSelectedInUse = ctx.freshName("initialSelectedInUse")
    val initialSize = ctx.freshName("initialSize")
    val initialSelected = ctx.freshName("initialSelected")
    val sizeAfterFirstChild = ctx.freshName("sizeAfterFirstChild")
    val selectedAfterFirstChild = ctx.freshName("selectedAfterFirstChild")
    val curSelected = ctx.freshName("curSelected")
    val tmp = ctx.freshName("tmp")
    val unselected = ctx.freshName("unselected")
    val unselectedSize = ctx.freshName("unselectedSize")
    val newSize = ctx.freshName("newSize")
    val k = ctx.freshName("k")
    s"""
      int $initialSize = ${ctx.INPUT_ROWBATCH}.size;
      boolean $initialSelectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      int[] $curSelected = ${ctx.INPUT_ROWBATCH}.selected;
      int[] $initialSelected = new int[${ctx.getBatchCapacity}];
      if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
        System.arraycopy($curSelected, 0, $initialSelected, 0, $initialSize);
      } else {
        for (int i = 0; i < $initialSize; i ++) {
          $initialSelected[i] = i;
          $curSelected[i] = i;
        }
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      ${eval1.code.trim}

      // Preserve the selected reference and size values generated
      // after the first child is evaluated.
      int $sizeAfterFirstChild = ${ctx.INPUT_ROWBATCH}.size;
      int[] $selectedAfterFirstChild = ${ctx.INPUT_ROWBATCH}.selected;
      int[] $tmp = new int[${ctx.getBatchCapacity}];
      int[] $unselected = new int[${ctx.getBatchCapacity}];

      // calculate unselected ones in last evaluate.
      for (int j = 0; j < $initialSize; j ++) {
        $tmp[$initialSelected[j]] = 0;
      }
      for (int j = 0; j < $sizeAfterFirstChild; j ++) {
        $tmp[$selectedAfterFirstChild[j]] = 1;
      }
      int $unselectedSize = 0;
      for (int j = 0; j < $initialSize; j ++) {
        int i = $initialSelected[j];
        if ($tmp[i] == 0) {
          $unselected[$unselectedSize ++] = i;
        }
      }

      ${ctx.INPUT_ROWBATCH}.selected = $unselected;
      ${ctx.INPUT_ROWBATCH}.size = $unselectedSize;

      ${eval2.code.trim}

      // Merge the results
      int $newSize = ${ctx.INPUT_ROWBATCH}.size + $sizeAfterFirstChild;
      for (int i = 0; i < ${ctx.INPUT_ROWBATCH}.size; i ++) {
        $tmp[${ctx.INPUT_ROWBATCH}.selected[i]] = 1;
      }
      int $k = 0;
      for (int j = 0; j < $initialSize; j ++) {
        int i = $initialSelected[j];
        if ($tmp[i] == 1) {
          ${ctx.INPUT_ROWBATCH}.selected[$k ++] = i;
        }
      }

      ${ctx.INPUT_ROWBATCH}.size = $newSize;
      if ($newSize == $initialSize) {
        ${ctx.INPUT_ROWBATCH}.selectedInUse = $initialSelectedInUse;
      }

    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchNot(
  child: BatchExpression,
  underlyingExpr: Expression) extends UnaryBatchExpression {

  override def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = child.genCode(ctx)
    val initialSize = ctx.freshName("initialSize")
    val initialSelected = ctx.freshName("initialSelected")
    val sizeAfterFirstChild = ctx.freshName("sizeAfterFirstChild")
    val selectedAfterFirstChild = ctx.freshName("selectedAfterFirstChild")
    val curSelected = ctx.freshName("curSelected")
    val tmp = ctx.freshName("tmp")
    val unselected = ctx.freshName("unselected")
    val unselectedSize = ctx.freshName("unselectedSize")
    s"""
      int $initialSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $curSelected = ${ctx.INPUT_ROWBATCH}.selected;
      int[] $initialSelected = new int[${ctx.getBatchCapacity}];
      if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
        System.arraycopy($curSelected, 0, $initialSelected, 0, $initialSize);
      } else {
        for (int i = 0; i < $initialSize; i ++) {
          $initialSelected[i] = i;
          $curSelected[i] = i;
        }
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      ${eval.code.trim}

      // Preserve the selected reference and size values generated
      // after the first child is evaluated.
      int $sizeAfterFirstChild = ${ctx.INPUT_ROWBATCH}.size;
      int[] $selectedAfterFirstChild = ${ctx.INPUT_ROWBATCH}.selected;
      int[] $tmp = new int[${ctx.getBatchCapacity}];
      int[] $unselected = new int[${ctx.getBatchCapacity}];

      // calculate unselected ones in last evaluate.
      for (int j = 0; j < $initialSize; j ++) {
        $tmp[$initialSelected[j]] = 0;
      }
      for (int j = 0; j < $sizeAfterFirstChild; j ++) {
        $tmp[$selectedAfterFirstChild[j]] = 1;
      }
      int $unselectedSize = 0;
      for (int j = 0; j < $initialSize; j ++) {
        int i = $initialSelected[j];
        if ($tmp[i] == 0) {
          $unselected[$unselectedSize ++] = i;
        }
      }

      ${ctx.INPUT_ROWBATCH}.selected = $unselected;
      ${ctx.INPUT_ROWBATCH}.size = $unselectedSize;
      ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}
// scalastyle:on
