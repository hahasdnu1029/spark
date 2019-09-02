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

import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GeneratedBatchExpressionCode}
import org.apache.spark.sql.catalyst.vector.{ColumnVector, RowBatch}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/*case class VectorsToRow(
  children: Seq[BatchExpression], underlyingExpr: Expression) extends BatchExpression {

  override def eval(input: RowBatch): ColumnVector = throw new UnsupportedOperationException

  override protected def genCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val numFields = children.size
    val numVarFields = children.map(_.dataType).filterNot(UnsafeRow.isFixedLength(_)).size

    val childrenDts: Seq[DataType] = children.map(_.dataType)
    val childrenCodes = children.map(_.genCode(ctx))

    val vectorGenClass = classOf[UnsafeRowVectorWriter].getName

    val vectorGen = ctx.freshName("vectorGen")
    ctx.addMutableState(vectorGenClass, vectorGen, v=>s"$v = " +
      s"new $vectorGenClass(${ctx.getBatchCapacity}, $numFields, $numVarFields);")

    ctx.references += this.underlyingExpr

    val evals = childrenDts.zip(childrenCodes).zipWithIndex.map { case ((dt, eval), index) =>
      val write = dt match {
        case IntegerType => s"$vectorGen.writeColumnInteger($index, ${eval.value});"
        case LongType => s"$vectorGen.writeColumnLong($index, ${eval.value});"
        case DoubleType => s"$vectorGen.writeColumnDouble($index, ${eval.value});"
        case StringType => s"$vectorGen.writeColumnUTF8String($index, ${eval.value});"
        case _ => "Not Implemented"
      }
      s"""
        ${eval.code}
        $write
      """
    }.mkString("\n")
    s"""
      $vectorGen.reset(${ctx.INPUT_ROWBATCH});
      OnColumnVector ${ev.value} =
        ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType, ctx)};
      $evals
      ${ev.value}.rowVector = $vectorGen.evaluate();
    """
  }
}*/

case class BatchMurmur3Hash(
    children: Seq[BatchExpression],
    seed: Int,
    underlyingExpr: Expression) extends BatchExpression {

  def this(children: Seq[BatchExpression], underlyingExpr: Expression) =
    this(children, 42, underlyingExpr)

  override def dataType: DataType = IntegerType
  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = false

  override def eval(input: RowBatch): ColumnVector = null

  override protected def doGenCode(ctx: CodegenContext, ev: GeneratedBatchExpressionCode): String = {
    val hasher = classOf[Murmur3_x86_32].getName

    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val resultV = ctx.freshName("resultV")

    val childrenCode = children.map { child =>
      val cur = child.genCode(ctx)
      val dt = child.dataType
      val curV = ctx.freshName("curV")
      val on = ctx.freshName("on")
      dt match {
        case StringType =>
          val bao = Platform.BYTE_ARRAY_OFFSET
          s"""
            ${cur.code}
            OnColumnVector $on = (OnColumnVector) ${cur.value};
            byte[][] $curV = $on.bytesVector;
            if (${cur.value}.noNulls && ${cur.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashUnsafeBytes(
                    $curV[0], $on.starts[0] + $bao, $on.lengths[0], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashUnsafeBytes(
                    $curV[0], $on.starts[0] + $bao, $on.lengths[0], $resultV[i]);
                }
              }
            } else if (${cur.value}.isRepeating) {
              // all nulls, should not change hash value
            } else if (${cur.value}.noNulls) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashUnsafeBytes(
                    $curV[i], $on.starts[i] + $bao, $on.lengths[i], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashUnsafeBytes(
                    $curV[i], $on.starts[i] + $bao, $on.lengths[i], $resultV[i]);
                }
              }
            } else {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if (!${cur.value}.isNullAt(i)) {
                    $resultV[i] = $hasher.hashUnsafeBytes($curV[i],
                      $on.starts[i] + $bao, $on.lengths[i], $resultV[i]);
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if (!${cur.value}.isNullAt(i)) {
                    $resultV[i] = $hasher.hashUnsafeBytes($curV[i],
                      $on.starts[i] + $bao, $on.lengths[i], $resultV[i]);
                  }
                }
              }
            }
          """
        case IntegerType =>
          s"""
            ${cur.code}
            int[] $curV = ((OnColumnVector)${cur.value}).intVector;
            if (${cur.value}.noNulls && ${cur.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashInt($curV[0], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashInt($curV[0], $resultV[i]);
                }
              }
            } else if (${cur.value}.isRepeating) {
              // all nulls, should not change hash value
            } else if (${cur.value}.noNulls) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashInt($curV[i], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashInt($curV[i], $resultV[i]);
                }
              }
            } else {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if (!${cur.value}.isNullAt(i)) {
                    $resultV[i] = $hasher.hashInt($curV[i], $resultV[i]);
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if (!${cur.value}.isNullAt(i)) {
                    $resultV[i] = $hasher.hashInt($curV[i], $resultV[i]);
                  }
                }
              }
            }
          """
        case LongType =>
          s"""
            ${cur.code}
            long[] $curV = ((OnColumnVector)${cur.value}).longVector;
            if (${cur.value}.noNulls && ${cur.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashLong($curV[0], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashLong($curV[0], $resultV[i]);
                }
              }
            } else if (${cur.value}.isRepeating) {
              // all nulls, should not change hash value
            } else if (${cur.value}.noNulls) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashLong($curV[i], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashLong($curV[i], $resultV[i]);
                }
              }
            } else {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if (!${cur.value}.isNullAt(i)) {
                    $resultV[i] = $hasher.hashLong($curV[i], $resultV[i]);
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if (!${cur.value}.isNullAt(i)) {
                    $resultV[i] = $hasher.hashLong($curV[i], $resultV[i]);
                  }
                }
              }
            }
          """
        case DoubleType =>
          s"""
            ${cur.code}
            double[] $curV = ((OnColumnVector)${cur.value}).doubleVector;
            if (${cur.value}.noNulls && ${cur.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[0]), $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[0]), $resultV[i]);
                }
              }
            } else if (${cur.value}.isRepeating) {
              // all nulls, should not change hash value
            } else if (${cur.value}.noNulls) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[i]), $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[i]), $resultV[i]);
                }
              }
            } else {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if (!${cur.value}.isNullAt(i)) {
                    $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[i]), $resultV[i]);
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if (!${cur.value}.isNullAt(i)) {
                    $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[i]), $resultV[i]);
                  }
                }
              }
            }
          """
        case _ => "Not Supported yet"
      }
    }.mkString("\n")

    s"""
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      boolean $selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;

      OnColumnVector ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType)};
      int[] $resultV = ${ev.value}.intVector;
      java.util.Arrays.fill($resultV, $seed);

      $childrenCode
    """
  }

  /** ONE line description of this node with more information */
  override def verboseString: String = ???
}
