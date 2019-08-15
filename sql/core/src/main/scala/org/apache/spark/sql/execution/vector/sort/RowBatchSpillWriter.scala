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

import java.io.File

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.GenerateBatchWrite
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.vector.DirectRowBatchSerializer
import org.apache.spark.storage.{BlockId, BlockManager}

case class RowBatchSpillWriter(
    blockManager: BlockManager,
    writeMetrics: ShuffleWriteMetrics,
    output: Seq[Attribute],
    defaultCapacity: Int) {

  var numBatchesSpilled = 0

  val (blockId: BlockId, file: File) = blockManager.diskBlockManager.createTempLocalBlock()

  val serializerInstance =
    (new DirectRowBatchSerializer(output, defaultCapacity, true, -1)).newInstance()
  var writer = blockManager.getDiskWriter(
    blockId, file, serializerInstance, 1024 * 1024, writeMetrics)

  val batchWrite = GenerateBatchWrite.generate(output, defaultCapacity)
  val sortedArray = (0 until defaultCapacity).toArray

  def write(rb: RowBatch): Unit = {
    numBatchesSpilled += 1

    rb.writer = batchWrite
    rb.sorted = sortedArray // we assume all batches written are full without sel
    rb.startIdx = 0
    rb.numRows = rb.size

    writer.write(null, rb)
  }

  def close(): Unit = {
    // write.commitAndClose()
    writer.close()
    writer = null
  }

  def getReader(blockManager: BlockManager): RowBatchSpillReader = {
    return new RowBatchSpillReader(
      blockManager, file, blockId, serializerInstance, numBatchesSpilled);
  }

}
