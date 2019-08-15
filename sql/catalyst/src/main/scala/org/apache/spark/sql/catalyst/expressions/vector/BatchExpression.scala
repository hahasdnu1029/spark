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

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, _}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.vector.{ColumnVector, RowBatch}
import org.apache.spark.sql.types._

abstract class BatchExpression extends TreeNode[BatchExpression] {

  def underlyingExpr: Expression

  /**
    * Returns true when an expression is a candidate for static evaluation before the query is
    * executed.
    *
    * The following conditions are used to determine suitability for constant folding:
    *  - A [[Coalesce]] is foldable if all of its children are foldable
    *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
    *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
    *  - A [[Literal]] is foldable
    *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
    */
  def foldable: Boolean = underlyingExpr.foldable

  /**
    * Returns true when the current expression always return the same result for fixed inputs from
    * children.
    *
    * Note that this means that an expression should be considered as non-deterministic if:
    * - if it relies on some mutable internal state, or
    * - if it relies on some implicit input that is not part of the children expression list.
    * - if it has non-deterministic child or children.
    *
    * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
    * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
    */
  def deterministic: Boolean = underlyingExpr.deterministic

  def nullable: Boolean = underlyingExpr.nullable

  def references: AttributeSet = AttributeSet(children.flatMap(_.references.iterator))

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: RowBatch = null): ColumnVector

  /**
    * Returns an [[org.apache.spark.sql.catalyst.expressions.codegen.GeneratedBatchExpressionCode]], which contains Java source code that
    * can be used to generate the result of evaluating the expression on an input row.
    *
    * @param ctx a [[org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext]]
    * @return [[GeneratedBatchExpressionCode]]
    */
  def genCode(ctx: CodegenContext): GeneratedBatchExpressionCode = {
    ctx.subBExprEliminationExprs.get(this).map { subEBExprState =>
      // This expression is repeated meaning the code to evaluated has already been added
      // as a function and called in advance. Just use it.
      val code = s"/* ${this.toCommentSafeString} */"
      GeneratedBatchExpressionCode(code, subEBExprState.value)
    }.getOrElse {
      val primitive = ctx.freshName("primitive")
      val ve = GeneratedBatchExpressionCode("", primitive)
      ve.code = doGenCode(ctx, ve)
      // Add `this` in the comment.
      ve.copy(s"/* ${this.toCommentSafeString} */\n" + ve.code.trim)
    }
  }

  /**
    * Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodegenContext]]
    * @param ev an [[GeneratedBatchExpressionCode]] with unique terms.
    * @return Java source code
    */
  protected def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String

  /**
    * Returns the [[DataType]] of the result of evaluating this expression.  It is
    * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
    */
  def dataType: DataType = underlyingExpr.dataType

  /**
    * Returns true when two expressions will always compute the same result, even if they differ
    * cosmetically (i.e. capitalization of names in attributes may be different).
    */
  def semanticEquals(other: BatchExpression): Boolean =
    underlyingExpr.semanticEquals(other.underlyingExpr)

  /**
    * Returns the hash for this expression. Expressions that compute the same result, even if
    * they differ cosmetically should return the same hash.
    */
  def semanticHash(): Int = underlyingExpr.semanticHash()

  /**
    * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
    * or returns a `TypeCheckResult` with an error message if invalid.
    * Note: it's not valid to call this method until `childrenResolved == true`.
    */
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /**
    * Returns a user-facing string representation of this expression's name.
    * This should usually match the name of the function in SQL.
    */
  def prettyName: String = getClass.getSimpleName.toLowerCase

  /**
    * Returns a user-facing string representation of this expression, i.e. does not have developer
    * centric debugging information like the expression id.
    */
  def prettyString: String = {
    "[vector]" + underlyingExpr.transform {
      case a: AttributeReference => PrettyAttribute(a.name, a.dataType)
      case u: UnresolvedAttribute => PrettyAttribute(u.name)
    }.toString
  }

  private def flatArguments = productIterator.flatMap {
    case t: Traversable[_] => t
    case single => single :: Nil
  }

  override def simpleString: String = toString

  override def toString: String = prettyName + flatArguments.mkString("(", ",", ")")

  /**
    * Returns the string representation of this expression that is safe to be put in
    * code comments of generated code.
    */
  protected def toCommentSafeString: String = this.toString
    .replace("*/", "\\*\\/")
    .replace("\\u", "\\\\u")

  var generateOffHeapColumnVector: Boolean = false
}

/**
  * A leaf expression, i.e. one without any child expressions.
  */
abstract class LeafBatchExpression extends BatchExpression {

  def children: Seq[BatchExpression] = Nil
}


/**
  * An expression with one input and one output. The output is by default evaluated to null
  * if the input is evaluated to null.
  */
abstract class UnaryBatchExpression extends BatchExpression {

  def child: BatchExpression

  override def children: Seq[BatchExpression] = child :: Nil

  override def foldable: Boolean = child.foldable

  override def nullable: Boolean = child.nullable

  /**
    * Default behavior of evaluation according to the default nullability of UnaryExpression.
    * If subclass of UnaryExpression override nullable, probably should also override this.
    */
  override def eval(input: RowBatch): ColumnVector = {
    // sys.error(s"UnaryExpressions must override either eval or nullSafeEval")
    child.eval(input)
  }

  /**
    * Called by unary expressions to generate a code block that returns null if its parent returns
    * null, and if not not null, use `f` to generate the expression.
    *
    * As an example, the following does a boolean inversion (i.e. NOT).
    * {{{
    *   defineCodeGen(ctx, ev, c => s"!($c)")
    * }}}
    *
    * @param f function that accepts a variable name and returns Java code to compute the output.
    */
  protected def defineCodeGen(
                               ctx: CodegenContext,
                               ev: GeneratedBatchExpressionCode,
                               f: String => String): String = {
    val eval = child.genCode(ctx)
    eval.code // TODO: just a place-holder here
  }
}


/**
  * An expression with two inputs and one output. The output is by default evaluated to null
  * if any input is evaluated to null.
  */
abstract class BinaryBatchExpression extends BatchExpression {

  def left: BatchExpression

  def right: BatchExpression

  override def children: Seq[BatchExpression] = Seq(left, right)

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
    * Default behavior of evaluation according to the default nullability of BinaryExpression.
    * If subclass of BinaryExpression override nullable, probably should also override this.
    */
  override def eval(input: RowBatch): ColumnVector = {
    left.eval(input) // TODO: just a place-holder here
  }

  /**
    * Short hand for generating binary evaluation code.
    * If either of the sub-expressions is null, the result of this computation
    * is assumed to be null.
    *
    * @param f accepts two variable names and returns Java code to compute the output.
    */
  protected def defineCodeGen(
                               ctx: CodegenContext,
                               ev: GeneratedBatchExpressionCode,
                               f: (String, String) => String): String = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    "" // TODO: just a place-holder here
  }
}


/**
  * A [[BinaryExpression]] that is an operator, with two properties:
  *
  * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
  * 2. Two inputs are expected to the be same type. If the two inputs have different types,
  * the analyzer will find the tightest common type and do the proper type casting.
  */
abstract class BinaryBatchOperator extends BinaryBatchExpression {

  /**
    * Expected input type from both left/right child expressions, similar to the
    * [[ImplicitCastInputTypes]] trait.
    */
  def inputType: AbstractDataType

  def symbol: String

  override def toString: String = s"($left $symbol[Batch] $right)"

  override def checkInputDataTypes(): TypeCheckResult = {
    // First check whether left and right have the same type, then check if the type is acceptable.
    if (left.dataType != right.dataType) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$prettyString' " +
        s"(${left.dataType.simpleString} and ${right.dataType.simpleString}).")
    } else if (!inputType.acceptsType(left.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"'$prettyString' requires ${inputType.simpleString} type," +
        s" not ${left.dataType.simpleString}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }
}


private[sql] object BinaryBatchOperator {
  def unapply(e: BinaryBatchOperator): Option[(BatchExpression, BatchExpression)] =
    Some((e.left, e.right))
}

/**
  * An expression with three inputs and one output. The output is by default evaluated to null
  * if any input is evaluated to null.
  */
abstract class TernaryBatchExpression extends BatchExpression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
    * Default behavior of evaluation according to the default nullability of TernaryExpression.
    * If subclass of BinaryExpression override nullable, probably should also override this.
    */
  override def eval(input: RowBatch): ColumnVector = {
    null // TODO: just a place-holder here
  }

  /**
    * Short hand for generating binary evaluation code.
    * If either of the sub-expressions is null, the result of this computation
    * is assumed to be null.
    *
    * @param f accepts two variable names and returns Java code to compute the output.
    */
  protected def defineCodeGen(
                               ctx: CodegenContext,
                               ev: GeneratedBatchExpressionCode,
                               f: (String, String, String) => String): String = {
    val evals = children.map(_.genCode(ctx))
    "" // TODO: just a place-holder here
  }
}
