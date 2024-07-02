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
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.{Partition, TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.arrow.vector.FieldVector
import org.apache.hadoop.fs.FileAlreadyExistsException

import scala.util.control.NonFatal

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
    var table: CatalogTable,
    partition: Map[String, Option[String]],
    outputColumns: Seq[Attribute])
  extends RDD[ColumnarBatch](prev) {

  private def collectNativeWriteOdpsMetrics(cb: ColumnarBatch): Option[ColumnarBatch] = {
    // Currently, the cb contains three columns: row, fragments, and context.
    // The first row in the row column contains the number of written numRows.
    // The fragments column contains detailed information about the file writes.
    print("Collect native write odps metrics.")

    val loadedCb = ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance, cb)

    /**
     * Column 0: name = rows Row 0, Column 0 value = 2 Row 1, Column 0 value = null
     *
     * Column 1: name = fragments Row 0, Column 1 value = null Row 1, Column 1 value = [B@24ba19e1
     *
     * Column 2: name = commitcontext Row 0, Column 2 value = [B@7981570b Row 1, Column 2 value =
     * [B@27ee7b5dIf
     */
    // Traverse each column and print its content
    for (colIdx <- 0 until loadedCb.numCols()) {
      val col = loadedCb.column(colIdx).asInstanceOf[ArrowWritableColumnVector]
      col.getValueVector match {
        case vector: FieldVector =>
          // Print column type and name
          print(s"Column $colIdx: name = ${vector.getField().getName()} \n")

          // Print each value in the column
          for (rowIdx <- 0 until loadedCb.numRows()) {
            val value = vector.getObject(rowIdx)
            var valueString = vector.getClass.getName
            if (value != null) {
              valueString = valueString + " : " + value.getClass.getName
            }
            print(s"Row $rowIdx, Column $colIdx value = ${valueString} \n")
          }
        case _ => print(s"Column $colIdx is not a FieldVector")
      }
    }
    Option.apply(loadedCb)
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
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath("writePath")
        // Initialize the native plan (WholeStageZippedPartitionsRDD)
        val iter = firstParent[ColumnarBatch].iterator(split, context)
        assert(iter.hasNext)

        val resultColumnarBatch = iter.next()
        assert(resultColumnarBatch != null)

        val nativeWriteTaskResult = collectNativeWriteOdpsMetrics(resultColumnarBatch)
        if (nativeWriteTaskResult.isEmpty) {
          println(
            "If we are writing an empty iterator, then velox would do nothing." +
              "Here we fallback to use vanilla Spark write files to generate an empty file for" +
              "metadata only.")
          // We have done commit task inside `WriteOdpsForEmptyIterator`.
          Iterator.empty
        } else {
          val writeTaskResult = nativeWriteTaskResult.get
          Iterator.single(writeTaskResult)
        }
      })(
        catchBlock = {
          // If there is an error, abort the task
          logError(s"Job aborted.")
          Iterator.empty
        }
      )
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        throw new TaskOutputFileAlreadyExistException(f)
      case e: Exception =>
        logError("Exception in compute method", e)
        Iterator.empty
    }
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
    var table: CatalogTable,
    partition: Map[String, Option[String]],
    outputColumns: Seq[Attribute],
    createTable: Boolean)
  extends UnaryExecNode
  with GlutenPlan {

  override lazy val references: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = Seq.empty

  final override lazy val supportsColumnar: Boolean = true

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (createTable) {
      createTableFirst(SparkSession.active, table)
    }
    assert(child.supportsColumnar)
    val rdd = child.executeColumnar()
    val veloxRdd = new VeloxColumnarWriteOdpsRDD(rdd, table, partition, outputColumns)
    // Collect the results and commit
    val collectedBatch = veloxRdd.collect()
    commit(collectedBatch)

    // Return the original RDD
    veloxRdd
  }

  def commit(batches: Array[ColumnarBatch]): Unit = {
    print("\ntrigger commit" + batches.length)
  }

  private def createTableFirst(sparkSession: SparkSession, tableDesc: CatalogTable): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val tableIdentifier = tableDesc.identifier
    val tableExists = catalog.tableExists(tableIdentifier)

    if (tableExists) {
      throw QueryCompilationErrors.tableIdentifierExistsError(tableIdentifier)
    } else {
      tableDesc.storage.locationUri.foreach {
        p =>
          DataWritingCommand.assertEmptyRootPath(
            p,
            SaveMode.ErrorIfExists,
            sparkSession.sessionState.newHadoopConf)
      }
      // TODO ideally, we should get the output data ready first and then
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      val tableSchema =
        CharVarcharUtils.getRawSchema(outputColumns.toStructType, sparkSession.sessionState.conf)
      assert(tableDesc.schema.isEmpty)
      catalog.createTable(tableDesc.copy(schema = tableSchema), ignoreIfExists = false)

      try {
        // Read back the metadata of the table which was created just now.
        table = catalog.getTableMetadata(tableDesc.identifier)
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          catalog.dropTable(tableIdentifier, ignoreIfNotExists = true, purge = false)
          throw e
      }
    }
  }

  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWriteFilesTransformerMetrics(sparkContext)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    VeloxColumnarWriteOdpsExec(newChild, table, partition, outputColumns, createTable)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new GlutenException(s"$nodeName does not support doExecute")
  }
}

object VeloxColumnarWriteOdpsExec {

  def apply(
      child: SparkPlan,
      table: CatalogTable,
      partition: Map[String, Option[String]],
      outputColumns: Seq[Attribute],
      createTable: Boolean): VeloxColumnarWriteOdpsExec = {

    new VeloxColumnarWriteOdpsExec(child, table, partition, outputColumns, createTable)
  }
}
