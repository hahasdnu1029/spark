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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types._

abstract class BatchInsertion {
  def insert(from: RowBatch, to: RowBatch, start: Int, length: Int): Unit
}

object GenerateBatchInsertion extends CodeGenerator[Seq[Expression], BatchInsertion] {
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in
  override protected def bind(
    in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] = in

  def generate(in: Seq[Expression], defaultCapacity: Int): BatchInsertion = {
    create(canonicalize(in), defaultCapacity)
  }

  override protected def create(in: Seq[Expression]): BatchInsertion =
    create(in, RowBatch.DEFAULT_CAPACITY)

  protected def create(in: Seq[Expression], defaultCapacity: Int): BatchInsertion = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val schema = in.map(_.dataType)

    val columnInsertions = schema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.buffers[$idx].serializeIntCV(from.columns[$idx], from.sorted, start, length);"
        case LongType =>
          s"to.buffers[$idx].serializeLongCV(from.columns[$idx], from.sorted, start, length);"
        case DoubleType =>
          s"to.buffers[$idx].serializeDoubleCV(from.columns[$idx], from.sorted, start, length);"
        case StringType =>
          s"to.buffers[$idx].serializeStringCV(from.columns[$idx], from.sorted, start, length);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificBatchInsertion(references);
      }

      class SpecificBatchInsertion extends ${classOf[BatchInsertion].getName} {
        private Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificBatchInsertion(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void insert(RowBatch from, RowBatch to, int start, int length) {
          to.size += length;
          $columnInsertions
        }
      }
    """
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated BatchInsertion by ${in.mkString(",")}:\n${CodeFormatter.format(code)}")
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BatchInsertion]
  }
}
