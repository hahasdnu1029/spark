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

import java.io.IOException
import java.nio.channels.ReadableByteChannel

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.expressions.vector.GenerateBatchOrdering.exprType
import org.apache.spark.sql.catalyst.expressions.vector.GenerateBatchWrite.logDebug
import org.apache.spark.sql.catalyst.vector.{ColumnVectorSerDeHelper, RowBatch}
import org.apache.spark.sql.types._

abstract class BatchRead {
  @throws(classOf[IOException])
  def append(in: ReadableByteChannel, rb: RowBatch, startIdx: Int, numRows: Int): Unit
}

object GenerateBatchRead extends CodeGenerator[Seq[Expression], BatchRead] {
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in
  override protected def bind(
    in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] = in

  def generate(expressions: Seq[Expression], defaultCapacity: Int): BatchRead =
    create(expressions, defaultCapacity)

  override protected def create(in: Seq[Expression]): BatchRead =
    create(in, RowBatch.DEFAULT_CAPACITY)

  protected def create(in: Seq[Expression], defaultCapacity: Int): BatchRead = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val bufferType = classOf[ColumnVectorSerDeHelper].getName

    val schema = in.map(_.dataType)

    val allocateBuffers = schema.zipWithIndex.map { case (dt, idx) =>
      s"""
        buffers[$idx] = $bufferType.create${ctx.typeName(dt)}Buffer($defaultCapacity);
      """
    }.mkString("\n")

    val columnsRead = schema.zipWithIndex.map { case (dt, idx) =>
      s"""
        buffers[$idx].populate${ctx.typeName(dt)}CV(rb.columns[$idx], in, startIdx, numRows);
      """
    }.mkString("\n")

    val codeBody =
      s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificBatchRead(references);
      }

      class SpecificBatchRead extends ${classOf[BatchRead].getName} {
        private  Object[] references;
        private $bufferType[] buffers;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificBatchRead(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
          this.buffers = new $bufferType[${schema.size}];
          $allocateBuffers
        }

        public void append(java.nio.channels.ReadableByteChannel in,
            RowBatch rb, int startIdx, int numRows) throws java.io.IOException{
          $columnsRead
        }
      }
    """
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated BatchRead by ${in.mkString(",")}':\n${CodeFormatter.format(code)}")
    // 对生成的代码进行编译，返回一个二元组（clazz,_）。clazz是class字节码对象
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BatchRead]
  }
}
