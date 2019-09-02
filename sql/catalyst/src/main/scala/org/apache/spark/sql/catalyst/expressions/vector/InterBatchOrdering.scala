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

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.vector.GenerateBatchOrdering
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, SortOrder}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types.StringType

abstract class InterBatchOrdering {
  def compare(lb: RowBatch, li: Int, rb: RowBatch, ri: Int): Int
}

object GenerateInterBatchOrdering extends CodeGenerator[Seq[SortOrder], InterBatchOrdering] {
  protected def canonicalize(in: Seq[SortOrder]): Seq[SortOrder] =
    in.map(ExpressionCanonicalizer.execute(_).asInstanceOf[SortOrder])

  protected def bind(in: Seq[SortOrder], inputSchema: Seq[Attribute]): Seq[SortOrder] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def generate(
      expressions: Seq[SortOrder],
      inputSchema: Seq[Attribute],
      defaultCapacity: Int): InterBatchOrdering = {
    create(canonicalize(bind(expressions, inputSchema)), defaultCapacity)
  }

  // TODO: we assume not null, not repeat columns are being compared
  def genComparisons(ctx: CodegenContext, ordering: Seq[SortOrder]): String = {
    ordering.map { order =>
      ctx.INPUT_ROWBATCH = "lb"
      val evalL = exprToBatch(order.child).genCode(ctx)
      ctx.INPUT_ROWBATCH = "rb"
      val evalR = exprToBatch(order.child).genCode(ctx)
      val asc = order.direction == Ascending
      val dt = order.child.dataType
      val pa = ctx.freshName("primitiveA")
      val pb = ctx.freshName("primitiveB")
      val get = ctx.getMethodName(dt)
      dt match {
        case StringType =>
          s"""
            ${evalL.code}
            ${evalR.code}
            UTF8String $pa;
            UTF8String $pb;

            $pa = ${evalL.value}.$get(li);
            $pb = ${evalR.value}.$get(ri);
            comp = ${ctx.genComp(dt, pa, pb)};
            if (comp != 0) {
              return ${if (asc) "comp" else "-comp"};
            }
          """
        case _ =>
          s"""
            ${evalL.code}
            ${evalR.code}
            ${CodeGenerator.javaType(dt)} $pa;
            ${CodeGenerator.javaType(dt)} $pb;
            $pa = ${evalL.value}.$get(li);
            $pb = ${evalR.value}.$get(ri);
            comp = ${ctx.genComp(dt, pa, pb)};
            if (comp != 0) {
              return ${if (asc) "comp" else "-comp"};
            }
          """
      }
    }.mkString("\n")
  }

  override protected def create(in: Seq[SortOrder]): InterBatchOrdering =
    create(in, RowBatch.DEFAULT_CAPACITY)

  protected def create(in: Seq[SortOrder], defaultCapacity: Int): InterBatchOrdering = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val comparisons = genComparisons(ctx, in)
    val codeBody = s"""
      public SpecificInterBatchOrdering generate(Object[] references) {
        return new SpecificInterBatchOrdering(references);
      }

      class SpecificInterBatchOrdering extends ${classOf[InterBatchOrdering].getName} {

        private Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificInterBatchOrdering(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public int compare(RowBatch lb, int li, RowBatch rb, int ri) {
          int comp;
          $comparisons
          return 0;
        }
      }
    """
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated InterBatchOrdering by ${in.mkString(",")}:\n${CodeFormatter.format(code)}")
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[InterBatchOrdering]
  }
}
