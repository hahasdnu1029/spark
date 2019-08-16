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

import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate.logDebug
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, Predicate}
import org.apache.spark.sql.catalyst.expressions.vector.GenerateBatchOrdering.exprType
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

abstract class BatchReorderer {
  def copy(from: RowBatch, to: RowBatch): Unit
}

object GenerateBatchReorderer extends CodeGenerator[Seq[Expression], BatchReorderer] {
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in
  override protected def bind(
    in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] = in

  def generate(in: Seq[Expression], defaultCapacity: Int): BatchReorderer = {
    create(canonicalize(in), defaultCapacity)
  }

  override protected def create(in: Seq[Expression]): BatchReorderer =
    create(in, RowBatch.DEFAULT_CAPACITY)

  protected def create(in: Seq[Expression], defaultCapacity: Int): BatchReorderer = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val schema = in.map(_.dataType)

    val columnCopiers = schema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType => s"to.columns[$idx].reorderInt(from.columns[$idx], from.sorted);"
        case LongType => s"to.columns[$idx].reorderLong(from.columns[$idx], from.sorted);"
        case DoubleType => s"to.columns[$idx].reorderDouble(from.columns[$idx], from.sorted);"
        case StringType => s"to.columns[$idx].reorderString(from.columns[$idx], from.sorted);"
        case _ => "Not implemented yet"
      }
    }.mkString("\n")

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificBatchReorderer(references);
      }

      class SpecificBatchReorderer extends ${classOf[BatchReorderer].getName} {
        private Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificBatchReorderer(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void copy(RowBatch from, RowBatch to) {
          $columnCopiers
        }
      }
    """
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated BatchReorderer by ${in.mkString(",")}:\n${CodeFormatter.format(code)}")

    // 对生成的代码进行编译，返回一个二元组（clazz,_）。clazz是class字节码对象
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BatchReorderer]
  }
}
