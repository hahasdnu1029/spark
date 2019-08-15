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
package org.apache.spark.sql.execution.vector.sort

import java.io._

import com.google.common.io.Closeables
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.storage.{BlockId, BlockManager}

case class RowBatchSpillReader(
    blockManager: BlockManager,
    file: File,
    blockId: BlockId,
    serializerInstance: SerializerInstance,
    numBatches: Int) extends RowBatchSorterIterator with Closeable {

  assert(file.length() > 0)

  val fi = new FileInputStream(file)
  var in: InputStream = null

  try {
    in = blockManager.wrapForCompression(blockId, fi)
  } catch {
    case ioe: IOException =>
      Closeables.close(fi, true)
      throw ioe
  }

  val kvIterator = serializerInstance.deserializeStream(in).asKeyValueIterator
  var rb: RowBatch = null

  override def hasNext(): Boolean = kvIterator.hasNext

  @throws[IOException]
  override def loadNext(): Unit = {
    rb = kvIterator.next()._2.asInstanceOf[RowBatch]
  }

  override def currentBatch: RowBatch = rb

  override def close(): Unit = {
    if (in != null) {
      try {
        in.close()
      } finally {
        in = null
      }
    }
  }
}
