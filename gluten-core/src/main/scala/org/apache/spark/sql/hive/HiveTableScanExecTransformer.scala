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
package org.apache.spark.sql.hive

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.BasicScanExecTransformer
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.HiveTableScanExecTransformer._
import org.apache.spark.sql.hive.execution.OdpsTableScanExec
import org.apache.spark.sql.odps.{OdpsClient, OdpsEmptyColumnPartition, OdpsScanPartition}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.ExpressionUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{ThreadUtils, Utils}

import com.aliyun.odps.PartitionSpec
import com.aliyun.odps.table.TableIdentifier
import com.aliyun.odps.table.configuration.{ArrowOptions, SplitOptions}
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.read.{TableBatchReadSession, TableReadSessionBuilder}
import com.aliyun.odps.table.read.split.InputSplit
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.mapred.TextInputFormat

import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit.MINUTES

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, NANOSECONDS}

@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
class HiveTableScanExecTransformer(
    @transient relation: HiveTableRelation,
    readDataColumns: Seq[Attribute],
    readPartitionColumns: Seq[Attribute],
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression])(session: SparkSession)
  extends OdpsTableScanExec(
    relation,
    readDataColumns,
    readPartitionColumns,
    partitionFilters,
    dataFilters)(session)
  with BasicScanExecTransformer {

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetrics(sparkContext)

  private val pushedDownFilters =
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, false))

  // odps table not support native scan predicate push down
  override def filterExprs(): Seq[Expression] = Seq.empty[Expression]

  override def getMetadataColumns(): Seq[AttributeReference] = Seq.empty

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] = partitions

  override def getPartitionSchema: StructType = relation.tableMeta.partitionSchema

  override def getDataSchema: StructType = relation.tableMeta.dataSchema

  override def getInputFilePathsInternal: Seq[String] = {
    Seq.empty
  }

  override protected def doValidateInternal(): ValidationResult = {
    ValidationResult.ok
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetricsUpdater(metrics)

  @transient private lazy val partitions: Seq[InputPartition] = createPartitions()

  /**
   * **** copy from OdpsTableScanExec start *****
   */
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal {
      batches =>
        new Iterator[ColumnarBatch] {

          override def hasNext: Boolean = {
            val startNs = System.nanoTime()
            val res = batches.hasNext
            scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
            res
          }

          override def next(): ColumnarBatch = {
            val batch = batches.next()
            numOutputRows += batch.numRows()
            batch
          }
        }
    }
  }

  private val requestedAttributes = readDataColumns ++ readPartitionColumns

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  @transient private lazy val dynamicallySelectedPartitions: Array[CatalogTablePartition] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      // call the file index for the files matching all filters except dynamic partition filters
      val partitionColumns = relation.partitionCols.toStructType
      val predicate = dynamicPartitionFilters.reduce(And)
      val boundPredicate = Predicate.create(
        predicate.transform {
          case a: AttributeReference =>
            val index = partitionColumns.indexWhere(a.name == _.name)
            BoundReference(index, partitionColumns(index).dataType, nullable = true)
        },
        Nil
      )
      val ret = selectedPartitions.filter(
        p => boundPredicate.eval(p.toRow(partitionColumns, conf.sessionLocalTimeZone)))
      ret
    } else {
      selectedPartitions
    }
  }
  private def createPartitions(): Array[InputPartition] = {
    if (relation.partitionCols.nonEmpty) {
      if (dynamicallySelectedPartitions.isEmpty) {
        return Array.empty
      }
    }
    val emptyColumn = requestedAttributes.isEmpty

    if (!emptyColumn) {
      if (relation.partitionCols.nonEmpty) {
        val partSplits = collection.mutable.Map[Int, ArrayBuffer[CatalogTablePartition]]()
        val splitPar = OdpsOptions.odpsSplitSessionParallelism(conf)
        val concurrentNum =
          Math.min(Math.max(splitPar, dynamicallySelectedPartitions.length / 200), 16)

        dynamicallySelectedPartitions.zipWithIndex.foreach {
          case (x, i) =>
            val key = if (concurrentNum == 1) 1 else i % concurrentNum
            partSplits.getOrElse(
              key, {
                val pList = ArrayBuffer[CatalogTablePartition]()
                partSplits.put(key, pList)
                pList
              }) += x
        }

        val future = Future.sequence(
          partSplits.keys.map(
            key =>
              Future[Array[InputPartition]] {
                val scan = createTableScan(emptyColumn, partSplits(key).toArray)
                scan.getInputSplitAssigner.getAllSplits
                  .map(split => OdpsScanPartition(split, scan))
              }(executionContext)))
        val futureResults = ThreadUtils.awaitResult(future, Duration(15, MINUTES))
        futureResults.flatten.toArray
      } else {
        val scan = createTableScan(emptyColumn, Array.empty)
        val partititons = scan.getInputSplitAssigner.getAllSplits
          .map(split => OdpsScanPartition(split, scan))
        logInfo(s"get partitions ${partititons.length}")
        partititons.asInstanceOf[Array[InputPartition]]
      }
    } else {
      val scan = if (relation.partitionCols.nonEmpty) {
        createTableScan(emptyColumn, dynamicallySelectedPartitions)
      } else {
        createTableScan(emptyColumn, Array.empty)
      }
      Array(OdpsEmptyColumnPartition(scan.getInputSplitAssigner.getTotalRowCount))
    }
  }

  private def divideEvenly(sessionId: String, totalCount: Long): Array[InputSplit] = {
    val parts = Array.fill(8)(new RowRangeInputSplit("EMPTY", 0, 0))
    if (totalCount < 4096 * 8) {
      parts(0) = new RowRangeInputSplit(sessionId, 0, totalCount)
      return parts.asInstanceOf[Array[InputSplit]]
    }
    val baseSize = totalCount / 8
    val remainder = totalCount % 8

    for (i <- 0 until 8) {
      val start = i * baseSize + math.min(i, remainder)
      val end = baseSize + (if (i < remainder) 1 else 0) - 1
      parts(i) = new RowRangeInputSplit(sessionId, start, end)
    }
    parts.asInstanceOf[Array[InputSplit]]
  }

  private def createTableScan(
      emptyColumn: Boolean,
      selectedPartitions: Array[CatalogTablePartition]): TableBatchReadSession = {
    val project = relation.tableMeta.database
    val table = relation.tableMeta.identifier.table
    // TODO: support three tier model
    val schema = "default"

    val settings = OdpsClient.get.getEnvironmentSettings
    val provider = OdpsOptions.odpsTableReaderProvider(conf)

    val requiredDataSchema = readDataColumns.map(attr => attr.name).asJava
    val requiredPartitionSchema = readPartitionColumns.map(attr => attr.name).asJava

    val scanBuilder = new TableReadSessionBuilder()
      .identifier(TableIdentifier.of(project, schema, table))
      .requiredDataColumns(requiredDataSchema)
      .requiredPartitionColumns(requiredPartitionSchema)
      .withSettings(settings)
      .withSessionProvider(provider)

    if (relation.partitionCols.nonEmpty) {
      scanBuilder.requiredPartitions(
        selectedPartitions
          .map(
            partition => {
              val staticPartition = new mutable.LinkedHashMap[String, String]
              relation.partitionCols.foreach {
                attr =>
                  staticPartition.put(
                    attr.name,
                    partition.spec.getOrElse(
                      attr.name,
                      throw new IllegalArgumentException(
                        s"Partition spec is missing a value for column '$attr.name': $partition"))
                  )
              }
              new PartitionSpec(
                staticPartition.map { case (key, value) => key + "=" + value }.mkString(","))
            })
          .toList
          .asJava)
    }

    val readSizeInBytes = relation.tableMeta.stats.get.sizeInBytes.longValue

    val splitOptions = if (!emptyColumn) {
      val rawSizePerCore = ((readSizeInBytes / 1024 / 1024) /
        SparkContext.getActive.get.defaultParallelism) + 1
      val sizePerCore = math.max(math.min(rawSizePerCore, Int.MaxValue).toInt, 10)
      val splitSizeInMB = math.min(OdpsOptions.odpsSplitSize(conf), sizePerCore)
      SplitOptions.newBuilder().SplitByByteSize(splitSizeInMB * 1024L * 1024L).build()
    } else {
      SplitOptions.newBuilder().SplitByRowOffset().build()
    }

    scanBuilder
      .withSplitOptions(splitOptions)
      .withArrowOptions(
        ArrowOptions
          .newBuilder()
          .withDatetimeUnit(TimestampUnit.MILLI)
          .withTimestampUnit(TimestampUnit.MICRO)
          .build())

    var scan: TableBatchReadSession = null
    val predicate = ExpressionUtil.convertToOdpsPredicate(pushedDownFilters)
    try {
      logInfo(s"Try to push down predicate $predicate")
      scan = scanBuilder.withFilterPredicate(predicate).buildBatchReadSession
    } catch {
      case e: Exception =>
        logWarning(s"Failed to push down predicate $predicate", e)
        scan = scanBuilder
          .withFilterPredicate(com.aliyun.odps.table.optimizer.predicate.Predicate.NO_PREDICATE)
          .buildBatchReadSession
    }

    logInfo(s"Create table scan ${scan.getId} for ${scan.getTableIdentifier}")
    scan
  }

  /**
   * **** copy from OdpsTableScanExec end *****
   */

  @transient override lazy val fileFormat: ReadFileFormat = {
    ReadFileFormat.OdpsReadFormat
  }

  override def nodeName: String = s"NativeScan odps ${relation.tableMeta.qualifiedName}"

  override def canEqual(other: Any): Boolean = other.isInstanceOf[HiveTableScanExecTransformer]

  override def equals(other: Any): Boolean = other match {
    case that: HiveTableScanExecTransformer =>
      that.canEqual(this) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def doCanonicalize(): HiveTableScanExecTransformer = {
    // FIXME: not sure if this is correct [zdx]
    // val canonicalized = super.doCanonicalize()
    val input: AttributeSeq = relation.output
    new HiveTableScanExecTransformer(
      relation.canonicalized.asInstanceOf[HiveTableRelation],
      readDataColumns.map(QueryPlan.normalizeExpressions(_, input)),
      readPartitionColumns.map(QueryPlan.normalizeExpressions(_, input)),
      QueryPlan.normalizePredicates(filterUnusedDynamicPruningExpressions(partitionFilters), input),
      QueryPlan.normalizePredicates(dataFilters, input)
    )(
      session
    )
  }

  private def filterUnusedDynamicPruningExpressions(
      predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }
}

object HiveTableScanExecTransformer {

  val NULL_VALUE: Char = 0x00
  val DEFAULT_FIELD_DELIMITER: Char = 0x01
  val TEXT_INPUT_FORMAT_CLASS: Class[TextInputFormat] =
    Utils.classForName("org.apache.hadoop.mapred.TextInputFormat")

  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  private val executionContext =
    ExecutionContext.fromExecutorService(newFixedThreadPool(16, namedThreadFactory("odps-scan")))

  def isOdpsTableScan(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[OdpsTableScanExec]
  }

  def getPartitionFilters(plan: SparkPlan): Seq[Expression] = {
    plan.asInstanceOf[OdpsTableScanExec].partitionFilters
  }

  def copyWith(plan: SparkPlan, newPartitionFilters: Seq[Expression]): SparkPlan = {
    val hiveTableScanExec = plan.asInstanceOf[OdpsTableScanExec]
    hiveTableScanExec.copy(partitionFilters = newPartitionFilters)(sparkSession =
      hiveTableScanExec.session)
  }

  def validate(plan: SparkPlan): ValidationResult = {
    plan match {
      case odpsTableScan: OdpsTableScanExec =>
        val hiveTableScanTransformer = new HiveTableScanExecTransformer(
          odpsTableScan.relation,
          odpsTableScan.readDataColumns,
          odpsTableScan.readPartitionColumns,
          odpsTableScan.partitionFilters,
          odpsTableScan.dataFilters
        )(odpsTableScan.session)
        hiveTableScanTransformer.doValidate()
      case _ => ValidationResult.notOk("Is not a ODPS scan")
    }
  }

  def apply(plan: SparkPlan): HiveTableScanExecTransformer = {
    plan match {
      case odpsTableScan: OdpsTableScanExec =>
        new HiveTableScanExecTransformer(
          odpsTableScan.relation,
          odpsTableScan.readDataColumns,
          odpsTableScan.readPartitionColumns,
          odpsTableScan.partitionFilters,
          odpsTableScan.dataFilters
        )(odpsTableScan.session)
      case _ =>
        throw new UnsupportedOperationException(
          s"Can't transform HiveTableScanExecTransformer from ${plan.getClass.getSimpleName}")
    }
  }
}
