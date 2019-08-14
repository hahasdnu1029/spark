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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/**
  * A row type that holds an array specialized container objects, of type [[MutableValue]], chosen
  * based on the dataTypes of each column.  The intent is to decrease garbage when modifying the
  * values of primitive columns.
  */
final class SpecificMutableRow(val values: Array[MutableValue])
  extends MutableRow with BaseGenericInternalRow {

  def this(dataTypes: Seq[DataType]) =
    this(
      dataTypes.map {
        case BooleanType => new MutableBoolean
        case ByteType => new MutableByte
        case ShortType => new MutableShort
        // We use INT for DATE internally
        case IntegerType | DateType => new MutableInt
        // We use Long for Timestamp internally
        case LongType | TimestampType => new MutableLong
        case FloatType => new MutableFloat
        case DoubleType => new MutableDouble
        case _ => new MutableAny
      }.toArray)

  def this() = this(Seq.empty)

  override def numFields: Int = values.length

  override def setNullAt(i: Int): Unit = {
    values(i).isNull = true
  }

  override def isNullAt(i: Int): Boolean = values(i).isNull


  override protected def genericGet(i: Int): Any = values(i).boxed

  override def update(ordinal: Int, value: Any) {
    if (value == null) {
      setNullAt(ordinal)
    } else {
      values(ordinal).update(value)
    }
  }

  override def copy(): GenericInternalRow = {
    val len = numFields
    val newValues = new Array[Any](len)
    var i = 0
    while (i < len) {
      newValues(i) = InternalRow.copyValue(genericGet(i))
      i += 1
    }
    new GenericInternalRow(newValues)
  }

  override def setInt(ordinal: Int, value: Int): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableInt]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getInt(i: Int): Int = {
    values(i).asInstanceOf[MutableInt].value
  }

  override def setFloat(ordinal: Int, value: Float): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableFloat]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getFloat(i: Int): Float = {
    values(i).asInstanceOf[MutableFloat].value
  }

  override def setBoolean(ordinal: Int, value: Boolean): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableBoolean]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getBoolean(i: Int): Boolean = {
    values(i).asInstanceOf[MutableBoolean].value
  }

  override def setDouble(ordinal: Int, value: Double): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableDouble]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getDouble(i: Int): Double = {
    values(i).asInstanceOf[MutableDouble].value
  }

  override def setShort(ordinal: Int, value: Short): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableShort]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getShort(i: Int): Short = {
    values(i).asInstanceOf[MutableShort].value
  }

  override def setLong(ordinal: Int, value: Long): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableLong]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getLong(i: Int): Long = {
    values(i).asInstanceOf[MutableLong].value
  }

  override def setByte(ordinal: Int, value: Byte): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableByte]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getByte(i: Int): Byte = {
    values(i).asInstanceOf[MutableByte].value
  }
}
