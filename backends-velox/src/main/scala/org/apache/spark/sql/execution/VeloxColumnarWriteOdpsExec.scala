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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.{Partition, SparkException, TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.FileAlreadyExistsException

case class VeloxWriteOdpsInfo(writeFileName: String, targetFileName: String, fileSize: Long)

case class VeloxWriteOdpsMetrics(
    name: String,
    updateMode: String,
    writePath: String,
    targetPath: String,
    fileWriteInfos: Seq[VeloxWriteOdpsInfo],
    rowCount: Long,
    inMemoryDataSizeInBytes: Long,
    onDiskDataSizeInBytes: Long,
    containsNumberedFileNames: Boolean)

// Velox write files metrics end

/**
 * This RDD is used to make sure we have injected staging write path before initializing the native
 * plan, and support Spark file commit protocol.
 */
class VeloxColumnarWriteOdpsRDD(
    var prev: RDD[ColumnarBatch],
    table: CatalogTable,
    partition: Map[String, Option[String]])
  extends RDD[ColumnarBatch](prev) {

  private def collectNativeWriteOdpsMetrics(cb: ColumnarBatch): Option[WriteTaskResult] = {
    // Currently, the cb contains three columns: row, fragments, and context.
    // The first row in the row column contains the number of written numRows.
    // The fragments column contains detailed information about the file writes.
    print("Collect native write odps metrics.")

    val loadedCb = ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance, cb)

    print("loadedCb: " + loadedCb.numRows() + " " + loadedCb.numCols())
    None
  }

  private def reportTaskMetrics(writeTaskResult: WriteTaskResult): Unit = {
    val stats = writeTaskResult.summary.stats.head.asInstanceOf[BasicWriteTaskStats]
    val (numBytes, numWrittenRows) = (stats.numBytes, stats.numRows)
    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    // We should update it after calling `commitTask` to overwrite the metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach {
      outputMetrics =>
        outputMetrics.setBytesWritten(numBytes)
        outputMetrics.setRecordsWritten(numWrittenRows)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val commitProtocol = new OdpsMockedCommitProtocol(table, partition)

    commitProtocol.setupTask()
    val writePath = commitProtocol.newTaskAttemptTempPath()
    print(s"Velox staging write path: $writePath")
    var writeTaskResult: WriteTaskResult = null
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath(writePath)

        // Initialize the native plan (WholeStageZippedPartitionsRDD)
        val iter = firstParent[ColumnarBatch].iterator(split, context)
        assert(iter.hasNext)
        val resultColumnarBatch = iter.next()
        assert(resultColumnarBatch != null)
        val nativeWriteTaskResult = collectNativeWriteOdpsMetrics(resultColumnarBatch)
        if (nativeWriteTaskResult.isEmpty) {
          print(
            "If we are writing an empty iterator, then velox would do nothing." +
              "Here we fallback to use vanilla Spark write files to generate an empty file for" +
              "metadata only.")
          // We have done commit task inside `WriteOdpsForEmptyIterator`.
        } else {
          writeTaskResult = nativeWriteTaskResult.get
          commitProtocol.commitTask()
        }
        iter
      })(
        catchBlock = {
          // If there is an error, abort the task
          commitProtocol.abortTask()
          logError(s"Job ${commitProtocol.getJobId} aborted.")
        }
      )
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw new SparkException(
          s"Task failed while writing rows to staging path: $writePath, " +
            s"output path: 'WriteFilesSpec.description.path'",
          t)
    }
//    assert(writeTaskResult != null)
//    reportTaskMetrics(writeTaskResult)
  }

  override protected def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}

// The class inherits from "BinaryExecNode" instead of "UnaryExecNode" because
// we need to expose a dummy child (as right child) with type "WriteOdpsExec" to let Spark
// choose the new write code path (version >= 3.4). The actual plan to write is the left child
// of this operator.
case class VeloxColumnarWriteOdpsExec private (
    override val child: SparkPlan,
    table: CatalogTable,
    partition: Map[String, Option[String]])
  extends UnaryExecNode
  with GlutenPlan {

  override lazy val references: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = Seq.empty

  final override lazy val supportsColumnar: Boolean = true

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // WholeStageTransformer
    assert(child.supportsColumnar)
    val rdd = child.executeColumnar()
    new VeloxColumnarWriteOdpsRDD(rdd, table, partition)
  }

  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWriteFilesTransformerMetrics(sparkContext)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    VeloxColumnarWriteOdpsExec(newChild, table, partition)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new GlutenException(s"$nodeName does not support doExecute")
  }
}

object VeloxColumnarWriteOdpsExec {

  def apply(
      child: SparkPlan,
      table: CatalogTable,
      partition: Map[String, Option[String]]): VeloxColumnarWriteOdpsExec = {

    new VeloxColumnarWriteOdpsExec(child, table, partition)
  }
}
