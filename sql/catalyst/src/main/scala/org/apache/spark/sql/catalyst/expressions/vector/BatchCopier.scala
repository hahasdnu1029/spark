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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.expressions.vector.GenerateBatchOrdering.{exprType, logDebug}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

abstract class BatchCopier {
  def copy(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int): Unit

  def copySortOnly(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int): Unit

  // column wise copy
  def copyCW(from: RowBatch, fromIdx: Int, num: Int,
             to: RowBatch, toIdx: Int, sorted: Array[Int]): Unit

  // column wise copy 2
  def copyCW2(all: Array[RowBatch], starts: Array[Int], num: Int,
              to: RowBatch, batchIds: Array[Int]): Unit
}

object GenerateBatchCopier extends CodeGenerator[Seq[Expression], BatchCopier] {
  var sortCols: Seq[Int] = null
  var nonSortCols: Seq[Int] = null

  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in

  override protected def bind(
                               in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] = in

  def generate(in: Seq[Expression], defaultCapacity: Int): BatchCopier = {
    create(canonicalize(in), defaultCapacity)
  }

  def generate(
                in: Seq[Expression], defaultCapacity: Int,
                sortColumns: Seq[Int], nonSortColumns: Seq[Int]): BatchCopier = {
    sortCols = sortColumns
    nonSortCols = nonSortColumns
    create(canonicalize(in), defaultCapacity)
  }

  override protected def create(in: Seq[Expression]): BatchCopier =
    create(in, RowBatch.DEFAULT_CAPACITY)

  protected def create(in: Seq[Expression], defaultCapacity: Int): BatchCopier = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val schema = in.map(_.dataType)

    val columnCopiers = schema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx].putInt(toIdx, from.columns[$idx].getInt(fromIdx));"
        case LongType =>
          s"to.columns[$idx].putLong(toIdx, from.columns[$idx].getLong(fromIdx));"
        case DoubleType =>
          s"to.columns[$idx].putDouble(toIdx, from.columns[$idx].getDouble(fromIdx));"
        case StringType =>
          s"to.columns[$idx].putString(toIdx, from.columns[$idx].getString(fromIdx));"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val sortColumnCopiers = if (sortCols == null) "" else {
      schema.zipWithIndex.filter { case (dt, idx) => sortCols.contains(idx) }.map { case (dt, idx) =>
        dt match {
          case IntegerType =>
            s"to.columns[$idx].putInt(toIdx, from.columns[$idx].getInt(fromIdx));"
          case LongType =>
            s"to.columns[$idx].putLong(toIdx, from.columns[$idx].getLong(fromIdx));"
          case DoubleType =>
            s"to.columns[$idx].putDouble(toIdx, from.columns[$idx].getDouble(fromIdx));"
          case StringType =>
            s"to.columns[$idx].putString(toIdx, from.columns[$idx].getString(fromIdx));"
          case _ =>
            "Not implemented yet"
        }
      }.mkString("\n")
    }

    val nonSortCopier = if (nonSortCols == null) "" else {
      val x = schema.zipWithIndex.filter { case (dt, idx) => nonSortCols.contains(idx) }.
        map { case (dt, idx) =>
          dt match {
            case IntegerType =>
              s"to.columns[$idx].putInt(sorted[toIdx + i], " +
                s"from.columns[$idx].getInt(fromIdx + i));"
            case LongType =>
              s"to.columns[$idx].putLong(sorted[toIdx + i], " +
                s"from.columns[$idx].getLong(fromIdx + i));"
            case DoubleType =>
              s"to.columns[$idx].putDouble(sorted[toIdx + i], " +
                s"from.columns[$idx].getDouble(fromIdx + i));"
            case StringType =>
              s"to.columns[$idx].putString(sorted[toIdx + i], " +
                s"from.columns[$idx].getString(fromIdx + i));"
            case _ =>
              "Not implemented yet"
          }
        }
      x.map { case sc =>
        s"""
           for (int i = 0; i < num; i ++) {
             $sc
           }
         """
      }.mkString("\n")
    }

    val copyByColumn = if (nonSortCols == null) "" else {
      val x = schema.zipWithIndex.filter { case (dt, idx) => nonSortCols.contains(idx) }.
        map { case (dt, idx) =>
          dt match {
            case IntegerType =>
              s"to.columns[$idx].putInt(i, cur.columns[$idx].getInt(cur.rowIdx));"
            case LongType =>
              s"to.columns[$idx].putLong(i, cur.columns[$idx].getLong(cur.rowIdx));"
            case DoubleType =>
              s"to.columns[$idx].putDouble(i, cur.columns[$idx].getDouble(cur.rowIdx));"
            case StringType =>
              s"to.columns[$idx].putString(i, cur.columns[$idx].getString(cur.rowIdx));"
            case _ =>
              "Not implemented yet"
          }
        }
      x.map { case sc =>
        s"""
          // reset rowIdx to origin
          for (int j = 0; j < batchNum; j++) {
            all[j].rowIdx = starts[j];
          }
          for (int i = 0; i < num; i++) {
            RowBatch cur = all[batchIds[i]];
            $sc
            cur.rowIdx += 1;
          }
        """
      }.mkString("\n")
    }

    val codeBody =
      s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificBatchCopier(references);
      }

      class SpecificBatchCopier extends ${classOf[BatchCopier].getName} {
        private Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public SpecificBatchCopier(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void copy(RowBatch from, int fromIdx, RowBatch to, int toIdx) {
          to.size += 1;
          $columnCopiers
        }

        public void copySortOnly(RowBatch from, int fromIdx, RowBatch to, int toIdx) {
          to.size += 1;
          $sortColumnCopiers
        }

        public void copyCW(RowBatch from, int fromIdx, int num,
          RowBatch to, int toIdx, int[] sorted) {
          $nonSortCopier
        }

        public void copyCW2(RowBatch[] all, int[] starts, int num,
            RowBatch to, int[] batchIds) {
          int batchNum = all.length;
          $copyByColumn
        }
      }
    """
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated BatchCopier by ${in.mkString(",")}:\n${CodeFormatter.format(code)}")
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BatchCopier]
  }
}
