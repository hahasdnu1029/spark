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

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.catalyst.expressions.{_}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.expressions.vector.GenerateBatchCopier.logDebug
import org.apache.spark.sql.catalyst.vector.RowBatch

abstract class BatchProjection {
  def apply(rowBatch: RowBatch): RowBatch
  def set(manager: TaskMemoryManager, consumer: MemoryConsumer): Unit
}

object BatchProjection {

  /**
   * Same as other create()'s but allowing enabling/disabling subexpression elimination.
   * TODO: refactor the plumbing and clean this up.
   */
  def create(
      exprs: Seq[Expression],
      inputSchema: Seq[Attribute],
      subexpressionEliminationEnabled: Boolean = false,
      defaultCapacity: Int = RowBatch.DEFAULT_CAPACITY,
      shouldReuseRowBatch: Boolean = true): BatchProjection = {
    val e = exprs.map(BindReferences.bindReference(_, inputSchema))
      .map(_ transform {
        case CreateStruct2(children) => CreateStructUnsafe2(children)
        case CreateNamedStruct2(children) => CreateNamedStructUnsafe2(children)
      })
    GenerateBatchProjection.generate(
      e, subexpressionEliminationEnabled, defaultCapacity, shouldReuseRowBatch)
  }
}

object GenerateBatchProjection extends CodeGenerator[Seq[Expression], BatchProjection] {
  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  /** Binds an input expression to a given input schema */
  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def generate(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean,
      defaultCapacity: Int,
      shouldReuseRowBatch: Boolean): BatchProjection = {
    if (shouldReuseRowBatch) {
      createWithRowBatchReuse(expressions, subexpressionEliminationEnabled, defaultCapacity)
    } else {
      createWithoutRowBatchReuse(expressions, subexpressionEliminationEnabled, defaultCapacity)
    }
  }

  /**
    * Generates a class for a given input expression.  Called when there is not cached code
    * already available.
    */
  override protected def create(in: Seq[Expression]): BatchProjection =
    createWithRowBatchReuse(in, false, RowBatch.DEFAULT_CAPACITY)

  private def createWithRowBatchReuse(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean,
      defaultCapacity: Int): BatchProjection = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)
    println(expressions.map(m=>s"======${m.nodeName}====="))
    val batchExpressions = expressions.map(exprToBatch)
    val exprEvals = ctx.generateBatchExpressions(batchExpressions, subexpressionEliminationEnabled)

    var result = ctx.freshName("result")
    result=ctx.addMutableState("RowBatch", result, v=>s"$v= new RowBatch(${expressions.size});")

    // Reset the subexpression values for each row.
    val subexprReset = ctx.subExprResetVariables.mkString("\n")

    val evals = exprEvals.zipWithIndex.map { case (eval, index) =>
      s"""
        ${eval.code}
        $result.columns[$index] = ${eval.value};
      """
    }.mkString("\n")


    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificBatchProjection(references);
      }

      class SpecificBatchProjection extends ${classOf[BatchProjection].getName} {

        private Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificBatchProjection(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void set(TaskMemoryManager manager, MemoryConsumer consumer) {}

        public java.lang.Object apply(java.lang.Object rowBatch) {
          return apply((RowBatch) rowBatch);
        }

        public RowBatch apply(RowBatch ${ctx.INPUT_ROWBATCH}) {
          $subexprReset
          $result.capacity = ${ctx.INPUT_ROWBATCH}.capacity;
          $result.size = ${ctx.INPUT_ROWBATCH}.size;
          $result.selected = ${ctx.INPUT_ROWBATCH}.selected;
          $result.selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
          $result.endOfFile = ${ctx.INPUT_ROWBATCH}.endOfFile;
          $result.sorted = ${ctx.INPUT_ROWBATCH}.sorted;
          $evals
          return $result;
        }
      }
      """
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated BatchProjection by ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BatchProjection]
  }

  private def createWithoutRowBatchReuse(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean,
      defaultCapacity: Int): BatchProjection = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val batchExpressions = expressions.map(exprToBatch)
    val exprEvals =
      ctx.generateBatchExpressions(batchExpressions, subexpressionEliminationEnabled, true)

    // Reset the subexpression values for each row.
    val subexprReset = ctx.subExprResetVariables.mkString("\n")

    val evals = exprEvals.zipWithIndex.map { case (eval, index) =>
      s"""
        ${eval.code}
        result.columns[$index] = ${eval.value};
      """
    }.mkString("\n")

    val estimatedBatchSize: Long = RowBatch.estimateMemoryFootprint(
      expressions.map(_.dataType).toArray, defaultCapacity, MemoryMode.OFF_HEAP)

    val allocateGranularity: Long = TaskMemoryManager.getAllocateGranularity(estimatedBatchSize);

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificBatchProjection(references);
      }

      class SpecificBatchProjection extends ${classOf[BatchProjection].getName} {

        private Object[] references;
        private TaskMemoryManager manager;
        private MemoryConsumer consumer;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificBatchProjection(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void set(TaskMemoryManager manager, MemoryConsumer consumer) {
          this.manager = manager;
          this.consumer = consumer;
        }

        public java.lang.Object apply(java.lang.Object rowBatch) {
          return apply((RowBatch) rowBatch);
        }

        public RowBatch apply(RowBatch ${ctx.INPUT_ROWBATCH}) {
          $subexprReset

          if (consumer == null || manager == null) {
            throw new RuntimeException("consumer or manager should be set first to apply for memory");
          }

          if (consumer.getAllocated() >= $estimatedBatchSize) {
            consumer.minusAllocated($estimatedBatchSize);
          } else {
            manager.acquireExecutionMemory(
              $allocateGranularity, MemoryMode.OFF_HEAP, consumer);
            consumer.addUsed($allocateGranularity);
            consumer.setAllocated($allocateGranularity - $estimatedBatchSize);
          }

          RowBatch result = new RowBatch(${expressions.size});

          result.capacity = ${ctx.INPUT_ROWBATCH}.capacity;
          result.size = ${ctx.INPUT_ROWBATCH}.size;
          result.selected = null; // ${ctx.INPUT_ROWBATCH}.selected;
          result.selectedInUse = false; // ${ctx.INPUT_ROWBATCH}.selectedInUse;
          result.endOfFile = ${ctx.INPUT_ROWBATCH}.endOfFile;
          result.sorted = (int[]) ${ctx.INPUT_ROWBATCH}.sorted.clone();
          $evals

          return result;
        }
      }
      """
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated BatchCopier by ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BatchProjection]

  }
}
