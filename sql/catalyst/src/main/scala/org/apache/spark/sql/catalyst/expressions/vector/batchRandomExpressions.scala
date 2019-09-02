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

import org.apache.spark.sql.catalyst.expressions.{Expression, Rand}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GeneratedBatchExpressionCode}
import org.apache.spark.sql.catalyst.vector.{ColumnVector, RowBatch}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}
import org.apache.spark.util.random.XORShiftRandom

abstract class BatchRDG extends LeafBatchExpression {

  protected def seed: Long

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType
}

case class BatchRand(seed: Long, r: Rand) extends BatchRDG {

  override def underlyingExpr: Expression = r

  override def eval(input: RowBatch): ColumnVector =
    throw new UnsupportedOperationException

  override protected def doGenCode(
    ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val rngTerm = ctx.freshName("rng")
    val className = classOf[XORShiftRandom].getName
    ctx.addMutableState(className, rngTerm,
      v=>s"$v = new $className(${seed}L + org.apache.spark.TaskContext.getPartitionId());")

    val batchSize = ctx.freshName("batchSize")

    val vectorGen: String = if (generateOffHeapColumnVector) {
      s"OffColumnVector ${ev.value} = " +
        s"${ctx.newOffVector(s"${ctx.INPUT_ROWBATCH}.capacity", DoubleType)};"
    } else {
      s"OnColumnVector ${ev.value} = " +
        s"${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", DoubleType)};"
    }

    s"""
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      $vectorGen
      ${ev.value}.noNulls = true;
      ${ev.value}.isRepeating = false;

      for (int i = 0; i < $batchSize; i ++) {
        ${ev.value}.putDouble(i, $rngTerm.nextDouble());
      }
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}

case class BatchRandInt(seed: Long, bound: Int, r: Rand) extends BatchRDG {

  override def dataType: DataType = IntegerType

  override def underlyingExpr: Expression = r

  override def eval(input: RowBatch): ColumnVector =
    throw new UnsupportedOperationException

  override protected def doGenCode(
    ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val rngTerm = ctx.freshName("rng")
    val className = classOf[XORShiftRandom].getName
    ctx.addMutableState(className, rngTerm,
      v=>s"$v = new $className(${seed}L + org.apache.spark.TaskContext.getPartitionId());")

    val batchSize = ctx.freshName("batchSize")

    val vectorGen: String = if (generateOffHeapColumnVector) {
      s"OffColumnVector ${ev.value} = " +
        s"${ctx.newOffVector(s"${ctx.INPUT_ROWBATCH}.capacity", IntegerType)};"
    } else {
      s"OnColumnVector ${ev.value} = " +
        s"${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", IntegerType)};"
    }

    s"""
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      $vectorGen
      ${ev.value}.noNulls = true;
      ${ev.value}.isRepeating = false;

      for (int i = 0; i < $batchSize; i ++) {
        ${ev.value}.putInt(i, $rngTerm.nextInt($bound));
      }
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}
