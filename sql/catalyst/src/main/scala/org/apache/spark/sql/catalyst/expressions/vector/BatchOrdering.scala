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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering.logDebug
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.vector.{IntComparator, RowBatch}
import org.apache.spark.sql.types._

/**
  * Inherits some default implementation for Java from `Ordering[Integer]`
  */
class BatchOrdering extends IntComparator {
  def compare(a: Int, b: Int): Int = {
    throw new UnsupportedOperationException
  }

  def reset(rb: RowBatch): Unit = {
    throw new UnsupportedOperationException
  }
}

object GenerateBatchOrdering extends CodeGenerator[Seq[SortOrder], BatchOrdering] {

  protected def canonicalize(in: Seq[SortOrder]): Seq[SortOrder] =
    in.map(ExpressionCanonicalizer.execute(_).asInstanceOf[SortOrder])

  protected def bind(in: Seq[SortOrder], inputSchema: Seq[Attribute]): Seq[SortOrder] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def generate(
      expressions: Seq[SortOrder],
      inputSchema: Seq[Attribute],
      defaultCapacity: Int): BatchOrdering = {
    create(canonicalize(bind(expressions, inputSchema)), defaultCapacity)
  }

  def genComparisons(ctx: CodegenContext, ordering: Seq[SortOrder]): String = {
    ordering.map { order =>
      val eval = exprToBatch(order.child).genCode(ctx)
      val asc = order.direction == Ascending
      val dt = order.child.dataType
      val pa = ctx.freshName("primitiveA")
      val pb = ctx.freshName("primitiveB")
      val get = ctx.getMethodName(dt)
      dt match {
        case StringType =>
          s"""
            ${eval.code}
            UTF8String $pa;
            UTF8String $pb;
            if (${eval.value}.isRepeating) {
              // Nothing
            } else if (${eval.value}.noNulls) {
              $pa = ${eval.value}.getString(a);
              $pb = ${eval.value}.getAnotherString(b);
              comp = ${ctx.genComp(dt, pa, pb)};
              if (comp != 0) {
                return ${if (asc) "comp" else "-comp"};
              }
            } else {
              if (${eval.value}.isNullAt(a) && ${eval.value}.isNullAt(b)) {
                // Nothing
              } else if (${eval.value}.isNullAt(a)) {
                return ${if (asc) "-1" else "1"};
              } else if (${eval.value}.isNullAt(b)) {
                return ${if (asc) "1" else "-1"};
              } else {
                $pa = ${eval.value}.getString(a);
                $pb = ${eval.value}.getAnotherString(b);
                comp = ${ctx.genComp(dt, pa, pb)};
                if (comp != 0) {
                  return ${if (asc) "comp" else "-comp"};
                }
              }
            }
          """
        case _ =>
          s"""
            ${eval.code}
            ${CodeGenerator.javaType(dt)} $pa;
            ${CodeGenerator.javaType(dt)} $pb;
            if (${eval.value}.isRepeating) {
              // Nothing
            } else if (${eval.value}.noNulls) {
              $pa = ${eval.value}.$get(a);
              $pb = ${eval.value}.$get(b);
              comp = ${ctx.genComp(dt, pa, pb)};
              if (comp != 0) {
                return ${if (asc) "comp" else "-comp"};
              }
            } else {
              if (${eval.value}.isNullAt(a) && ${eval.value}.isNullAt(b)) {
                // Nothing
              } else if (${eval.value}.isNullAt(a)) {
                return ${if (asc) "-1" else "1"};
              } else if (${eval.value}.isNullAt(b)) {
                return ${if (asc) "1" else "-1"};
              } else {
                $pa = ${eval.value}.$get(a);
                $pb = ${eval.value}.$get(b);
                comp = ${ctx.genComp(dt, pa, pb)};
                if (comp != 0) {
                  return ${if (asc) "comp" else "-comp"};
                }
              }
            }
          """
      }
    }.mkString("\n")
  }

  override protected def create(in: Seq[SortOrder]): BatchOrdering =
    create(in, RowBatch.DEFAULT_CAPACITY)

  protected def create(in: Seq[SortOrder], defaultCapacity: Int): BatchOrdering = {
    val ctx = newCodeGenContext()
    // 设置Batch的容量
    ctx.setBatchCapacity(defaultCapacity)
    // 生成比较器的代码
    val comparisons = genComparisons(ctx, in)
    val codeBody = s"""
      public SpecificBatchOrdering generate(Object[] references) {
        return new SpecificBatchOrdering(references);
      }

      class SpecificBatchOrdering extends ${classOf[BatchOrdering].getName} {

        private Object[] references;
        private RowBatch current;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificBatchOrdering(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void reset(RowBatch rb) {
          this.current = rb;
        }

        public int compare(int a, int b) {
          RowBatch ${ctx.INPUT_ROWBATCH} = current;
          int comp;
          $comparisons
          return 0;
        }
      }
    """
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated BatchOrdering by ${in.mkString(",")}:\n${CodeFormatter.format(code)}")
    println("===========================BatchOrderingCode===========================================")
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BatchOrdering]
  }
}

object BatchOrderings {

  def needFurtherCompare(origin: Seq[SortOrder]): Boolean = {
    if (origin.size == 1) {
      origin(0).dataType match {
        case _: IntegralType | FloatType | DoubleType => false
        case _ => true
      }
    } else {
      true
    }
  }
}
