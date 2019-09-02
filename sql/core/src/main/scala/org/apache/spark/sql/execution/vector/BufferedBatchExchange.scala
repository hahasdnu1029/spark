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

package org.apache.spark.sql.execution.vector

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.vector.{BatchProjection, GenerateBatchInsertion, GenerateBatchWrite}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{SparkSqlSerializer, UnaryExecNode, _}
import org.apache.spark.sql.execution.exchange.ExchangeCoordinator
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.{MutablePair, NextIterator}

case class BufferedBatchExchange(
                                  var newPartitioning: Partitioning,
                                  child: SparkPlan,
                                  @transient coordinator: Option[ExchangeCoordinator]) extends UnaryExecNode {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))

  override def nodeName: String = {
    val extraInfo = coordinator match {
      case Some(exchangeCoordinator) if exchangeCoordinator.isEstimated =>
        s"(coordinator id: ${System.identityHashCode(coordinator)})"
      case Some(exchangeCoordinator) if !exchangeCoordinator.isEstimated =>
        s"(coordinator id: ${System.identityHashCode(coordinator)})"
      case None => ""
    }
    s"BufferedBatchExchange$extraInfo"
  }

  /**
    * Returns true if we can support the data type, and we are not doing range partitioning.
    */
  private lazy val tungstenMode: Boolean = !newPartitioning.isInstanceOf[RangePartitioning]


  override def outputPartitioning: Partitioning = newPartitioning

  override def output: Seq[Attribute] = child.output

  override def outputsUnsafeRows: Boolean = false

  override def canProcessSafeRows: Boolean = false

  override def canProcessUnsafeRows: Boolean = false

  override def canProcessRowBatches: Boolean = true

  override def outputsRowBatches: Boolean = true

  @transient private lazy val sparkConf = child.sqlContext.sparkContext.getConf
  // bypassThreshod的阈值大小为200
  @transient private lazy val bypassThreshold =
    sparkConf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)

  // 默认的Batch容量
  private val defaultCapacity: Int = sqlContext.conf.vectorizedBatchCapacity

  setDefaultBatchCapacity(sqlContext.conf.vectorizedBatchCapacity)

  val rbSchema = output.map(_.dataType).toArray

  private lazy val serializer: Serializer = {
    if (tungstenMode) {
      new DirectRowBatchSerializer(output, defaultCapacity, shouldReuseRowBatch, this.getParentId())
    } else {
      new SparkSqlSerializer(sparkConf)
    }
  }

  override protected def doPrepare(): Unit = {
    // If an ExchangeCoordinator is needed, we register this Exchange operator
    // to the coordinator when we do prepare. It is important to make sure
    // we register this operator right before the execution instead of register it
    // in the constructor because it is possible that we create new instances of
    // Exchange operators when we transform the physical plan
    // (then the ExchangeCoordinator will hold references of unneeded Exchanges).
    // So, we should only call registerExchange just before we start to execute
    // the plan.
    coordinator match {
      case Some(exchangeCoordinator) => // exchangeCoordinator.registerExchange(this)
      case None =>
    }
  }

  /**
    * Returns a [[ShuffleDependency]] that will partition rows of its child based on
    * the partitioning scheme defined in `newPartitioning`. Those partitions of
    * the returned ShuffleDependency will be the input of shuffle.
    */
  private[sql] def prepareShuffleDependency():
  ShuffleDependency[Int, RowBatch, RowBatch] = {
    val rdd = child.batchExecute()
    val part: Partitioner = newPartitioning match {
      case HashPartitioning(_, n) =>
        new PartitionIdPassthrough(n)
      case _ => sys.error(s"BufferedBatchExchange not implemented for $newPartitioning")
    }

    // 返回一个BatchProjection，根据hash的key返回的所在分区id的Projection
    def getPartitionKeyExtractor(): BatchProjection = newPartitioning match {
      case h: HashPartitioning =>
        BatchProjection.create(
          h.partitionIdExpression :: Nil, child.output, false, defaultCapacity, true)
      case _ => sys.error(s"BufferedBatchExchange not implemented for $newPartitioning")
    }

    // bypass版本，根据partitionKeyExtractor重新组织RowBatch
    def bufferRowsByPartition(
                               iterator: Iterator[RowBatch],
                               numPartitions: Int,
                               partitionKeyExtractor: BatchProjection): Iterator[Product2[Int, RowBatch]] = {
      // 生成batchInsertion
      val batchInsertion = GenerateBatchInsertion.generate(output, defaultCapacity)

      new Iterator[Product2[Int, RowBatch]] {
        // 有多少个分区就有多少个rbBuffers
        val rbBuffers = new Array[RowBatch](numPartitions)
        // 进行多个rbBuffers聚合产生的
        val fullBatches = new mutable.Queue[(Int, RowBatch)]

        var currentPair: Product2[Int, RowBatch] = null

        def populateRBs(): Unit = {
          while (iterator.hasNext && fullBatches.isEmpty) {
            val current = iterator.next()
            // 得到进行分区的key的值(这里已经进行了MurMur3Hash了)
            val partitionKey = partitionKeyExtractor(current).columns(0).getIntVector
            // 根据partitionKey进行排序
            current.sort(partitionKey)
            // 进行数据的分区和重新组织
            var curPID: Int = -1 // currentPartitionId
            var numRows: Int = 0 // numRowsInCurrentPartition
            var startIdx: Int = -1 // currentPartitionStartIdx
            var j = 0
            while (j < current.size) {
              var i = current.sorted(j)
              curPID = partitionKey(i)
              startIdx = j

              while (partitionKey(i) == curPID && j < current.size - 1) {
                numRows += 1
                j += 1
                i = current.sorted(j)
              }
              if (partitionKey(i) == curPID && j == current.size - 1) {
                numRows += 1
                j += 1
              }

              if (rbBuffers(curPID) == null) {
                rbBuffers(curPID) = RowBatchPool.get()
              }
              if (rbBuffers(curPID).size + numRows > rbBuffers(curPID).capacity) {
                val recordsInserted = rbBuffers(curPID).capacity - rbBuffers(curPID).size
                // 将current插入到rbBuffers中从startIdx，数目recordsInserted
                batchInsertion.insert(current, rbBuffers(curPID), startIdx, recordsInserted)
                fullBatches.enqueue((curPID, rbBuffers(curPID)))
                rbBuffers(curPID) = RowBatchPool.get()
                batchInsertion.insert(
                  current, rbBuffers(curPID), startIdx + recordsInserted, numRows - recordsInserted)
              } else if (rbBuffers(curPID).size + numRows == rbBuffers(curPID).capacity) {
                batchInsertion.insert(current, rbBuffers(curPID), startIdx, numRows)
                fullBatches.enqueue((curPID, rbBuffers(curPID)))
                rbBuffers(curPID) = null
              } else {
                // 未达到容量上限， 将current插入到rbBuffers中从startIdx，数目recordsInserted
                batchInsertion.insert(current, rbBuffers(curPID), startIdx, numRows)
              }

              curPID = -1
              numRows = 0
            }
          }
        }

        override def hasNext: Boolean =
          iterator.hasNext || !fullBatches.isEmpty || rbBuffers.exists(_ != null)

        override def next(): Product2[Int, RowBatch] = {
          if (currentPair != null) {
            RowBatchPool.put(currentPair._2)
            currentPair = null
          }

          if (!fullBatches.isEmpty) {
            currentPair = fullBatches.dequeue()
          } else if (iterator.hasNext) {
            populateRBs()
            if (fullBatches.isEmpty) {
              var i = 0
              while (i < rbBuffers.size) {
                if (rbBuffers(i) != null) {
                  fullBatches.enqueue((i, rbBuffers(i)))
                  rbBuffers(i) = null
                }
                i += 1
              }
            }
            currentPair = fullBatches.dequeue()
          } else if (rbBuffers.exists(_ != null)) {
            var i = 0
            while (i < rbBuffers.size) {
              if (rbBuffers(i) != null) {
                fullBatches.enqueue((i, rbBuffers(i)))
                rbBuffers(i) = null
              }
              i += 1
            }
            currentPair = fullBatches.dequeue()
          } else {
            sys.error("we should never reach here")
          }
          currentPair
        }
      }
    }

    // sort版本
    def sortRowsByPartition(iterator: Iterator[RowBatch],
                            numPartitions: Int,
                            partitionKeyExtractor: BatchProjection): Iterator[Product2[Int, RowBatch]] = {
      val batchWrite = GenerateBatchWrite.generate(output, defaultCapacity)

      new NextIterator[Product2[Int, RowBatch]] {
        var currentBatch: RowBatch = null
        var currentPair = new MutablePair[Int, RowBatch]()

        var currentPID: Int = -1
        var numRows: Int = 0
        var startIdx: Int = -1
        var currentIdx: Int = 0
        var currentSize: Int = -1
        var currentPartitionKeys: Array[Int] = null

        def findNextRange(): Unit = {
          numRows = 0
          var i = currentBatch.sorted(currentIdx)
          currentPID = currentPartitionKeys(i)
          startIdx = currentIdx
          while (currentPartitionKeys(i) == currentPID && currentIdx < currentSize - 1) {
            numRows += 1
            currentIdx += 1
            i = currentBatch.sorted(currentIdx)
          }
          if (currentPartitionKeys(i) == currentPID && currentIdx == currentSize - 1) {
            numRows += 1
            currentIdx += 1
          }

          currentBatch.startIdx = startIdx
          currentBatch.numRows = numRows
          currentBatch.writer = batchWrite
          currentPair.update(currentPID, currentBatch)
        }

        override protected def getNext(): Product2[Int, RowBatch] = {
          if (currentBatch != null && startIdx + numRows < currentSize) {
            findNextRange()
          } else if (!iterator.hasNext) {
            finished = true
          } else {
            currentBatch = iterator.next()
            while (currentBatch.size == 0 && iterator.hasNext) {
              currentBatch = iterator.next()
            }
            if (currentBatch.size != 0) {
              currentPartitionKeys = partitionKeyExtractor(currentBatch).columns(0).getIntVector
              currentBatch.sort(currentPartitionKeys)

              currentPID = -1
              numRows = 0
              startIdx = -1
              currentIdx = 0
              currentSize = currentBatch.size

              findNextRange()
            } else {
              finished = true
            }
          }
          currentPair
        }

        override protected def close(): Unit = {}
      }
    }

    // bypass和sort
    val rddWithPartitionCV: RDD[Product2[Int, RowBatch]] = {
      if (part.numPartitions <= bypassThreshold) { // bypass version
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          bufferRowsByPartition(iter, part.numPartitions, getPartitionKey)
        }
      } else { // sort version
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          sortRowsByPartition(iter, part.numPartitions, getPartitionKey)
        }
      }
    }
    // 创建shuffleDependeces，rowBatchMode=true，这里会注册shuffle
    val dependency =
      new ShuffleDependency[Int, RowBatch, RowBatch](
        rddWithPartitionCV,
        part,
        serializer,
        rowBatchMode = true)

    dependency
  }

  /**
    * Returns a [[ShuffledRowBatchRDD]] that represents the post-shuffle dataset.
    * This [[ShuffledRowBatchRDD]] is created based on a given [[ShuffleDependency]] and an optional
    * partition start indices array. If this optional array is defined, the returned
    * [[ShuffledRowBatchRDD]] will fetch pre-shuffle partitions based on indices of this array.
    */
  private[sql] def preparePostShuffleRDD(
                                          shuffleDependency: ShuffleDependency[Int, RowBatch, RowBatch],
                                          specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowBatchRDD = {
    // If an array of partition start indices is provided, we need to use this array
    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
    // update the number of post-shuffle partitions.
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)
    }
    new ShuffledRowBatchRDD(shuffleDependency, specifiedPartitionStartIndices)
  }

  protected override def doBatchExecute(): RDD[RowBatch] = attachTree(this, "batchExecute") {
    coordinator match {
      case Some(exchangeCoordinator) =>
        // val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
        // assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
        // shuffleRDD
        null
      case None =>
        // 准备依赖关系ShuffleDependency
        val shuffleDependency = prepareShuffleDependency()
        // 准备ShuffleRDD
        preparePostShuffleRDD(shuffleDependency)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  object RowBatchPool {
    val pool = new mutable.Queue[RowBatch]

    def get(): RowBatch = {
      if (pool.isEmpty) {
        new RowBatch(rbSchema, defaultCapacity);
      } else {
        pool.dequeue()
      }
    }

    def put(rb: RowBatch): Unit = {
      rb.reset()
      pool.enqueue(rb)
    }
  }

}

object BufferedBatchExchange {
  def apply(newPartitioning: Partitioning, child: SparkPlan): BufferedBatchExchange = {
    BufferedBatchExchange(newPartitioning, child, coordinator = None: Option[ExchangeCoordinator])
  }
}
