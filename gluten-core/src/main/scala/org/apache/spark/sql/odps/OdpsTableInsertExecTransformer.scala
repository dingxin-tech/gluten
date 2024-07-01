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
package org.apache.spark.sql.odps

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{TransformContext, TransformSupport, UnaryTransformSupport}
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.{ColumnTypeNode, TypeBuilder}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{OdpsInsertHandle, RelBuilder, RelNode, WriteRelNode}

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.{CreateOdpsTableAsSelectCommand, InsertIntoOdpsTable}

import com.aliyun.odps.table.TableIdentifier
import com.aliyun.odps.table.configuration.ArrowOptions
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.write.{TableWriteCapabilities, TableWriteSessionBuilder}

import java.lang

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`;

/** @author dingxin (zhangdingxin.zdx@alibaba-inc.com) */
@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
case class OdpsTableInsertExecTransformer(
    child: SparkPlan,
    insertIntoOdpsTable: InsertIntoOdpsTable,
    createTable: Boolean)
  extends UnaryTransformSupport {

  val table: CatalogTable = insertIntoOdpsTable.table
  val partition: Map[String, Option[String]] = insertIntoOdpsTable.partition
  val query: LogicalPlan = insertIntoOdpsTable.query
  val overwrite: Boolean = insertIntoOdpsTable.overwrite
  val ifPartitionNotExists: Boolean = insertIntoOdpsTable.ifPartitionNotExists
  val outputColumnNames: Seq[String] = insertIntoOdpsTable.outputColumnNames
  val columns: Seq[Attribute] = insertIntoOdpsTable.outputColumns

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWriteFilesTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genWriteFilesTransformerMetricsUpdater(metrics)

  override def output: Seq[Attribute] = Seq.empty

  def getRelNode(
      context: SubstraitContext,
      attributes: Seq[Attribute],
      operatorId: lang.Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    if (table.tableType == CatalogTableType.EXTERNAL) {
      throw new SparkException(s"Unsupported table type for table write ${table.tableType}")
    }

    val columnTypeNodes = new java.util.ArrayList[ColumnTypeNode]()
    val inputAttributes = new java.util.ArrayList[Attribute]()

    val childSize = this.child.output.size
    for (i <- 0 until childSize) {
      columnTypeNodes.add(new ColumnTypeNode(0))
      inputAttributes.add(attributes(i))
    }

    val typeNodes = ConverterUtils.collectAttributeTypeNodes(attributes)

    val nameList =
      ConverterUtils.collectAttributeNames(inputAttributes.toSeq)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(createEnhancement(attributes))

    val node = RelBuilder.makeWriteRel(
      input,
      typeNodes,
      nameList,
      columnTypeNodes,
      extensionNode,
      context,
      operatorId)

    val settings = OdpsClient.get.getEnvironmentSettings
    // TODO: val provider = OdpsOptions.odpsTableReaderProvider(conf)

    val arrowOptions = ArrowOptions
      .newBuilder()
      .withDatetimeUnit(TimestampUnit.MILLI)
      .withTimestampUnit(TimestampUnit.MICRO)
      .build()

    val writeCapabilities = TableWriteCapabilities
      .newBuilder()
      .supportDynamicPartition(true)
      .supportHashBuckets(true)
      .supportRangeBuckets(true)
      .build()

    val sinkBuilder = new TableWriteSessionBuilder()
      .identifier(TableIdentifier.of(table.identifier.database.get, table.identifier.table))
      .withArrowOptions(arrowOptions)
      .withCapabilities(writeCapabilities)
      .withSettings(settings)

    val batchSink = sinkBuilder.buildBatchWriteSession()
    logInfo(s"Create table sink ${batchSink.getId} for ${batchSink.getTableIdentifier}")

    node
      .asInstanceOf[WriteRelNode]
      .setOdpsInsertHandle(
        new OdpsInsertHandle(
          table.identifier.database.get,
          "default",
          table.identifier.table,
          batchSink.getId))
    node
  }

  def createEnhancement(output: Seq[Attribute]): com.google.protobuf.Any = {
    val inputTypeNodes = output.map {
      attr =>
        {
          print("[debug] enhancement value: " + attr.toJSON)
          ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
        }
    }
    BackendsApiManager.getTransformerApiInstance.packPBMessage(
      TypeBuilder.makeStruct(false, inputTypeNodes.asJava).toProtobuf)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getRelNode(context, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Write Rel should be valid")
    print("currRel:" + currRel.toProtobuf.toString)
    TransformContext(childCtx.outputAttributes, output, currRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

object OdpsTableInsertExecTransformer {
  def isOdpsTableWrite(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[DataWritingCommandExec]
  }

  def validate(plan: SparkPlan): ValidationResult = {
    plan match {
      case dataWritingCommandExec: DataWritingCommandExec =>
        dataWritingCommandExec.cmd match {
          case a: InsertIntoOdpsTable =>
            print("is InsertIntoOdpsTable")
            ValidationResult.ok
          case b: CreateOdpsTableAsSelectCommand =>
            print("is CreateOdpsTableAsSelectCommand")
            ValidationResult.ok
          case _ =>
            ValidationResult.notOk("Is not InsertIntoOdpsTable/CreateOdpsTableAsSelectCommand")
        }
      case _ => ValidationResult.notOk("Is not InsertIntoOdpsTable")
    }
  }

  def apply(plan: SparkPlan): OdpsTableInsertExecTransformer = {
    plan match {
      case dataWritingCommandExec: DataWritingCommandExec =>
        dataWritingCommandExec.cmd match {
          case a: InsertIntoOdpsTable =>
            val insertIntoOdpsTable = dataWritingCommandExec.cmd.asInstanceOf[InsertIntoOdpsTable]
            new OdpsTableInsertExecTransformer(
              dataWritingCommandExec.child,
              insertIntoOdpsTable,
              false
            )
          case b: CreateOdpsTableAsSelectCommand =>
            val createOdpsTableAsSelectCommand =
              dataWritingCommandExec.cmd.asInstanceOf[CreateOdpsTableAsSelectCommand]
            val sparkSession = SparkSession.active
            new OdpsTableInsertExecTransformer(
              dataWritingCommandExec.child,
              createOdpsTableAsSelectCommand
                .getWritingCommand(
                  sparkSession.sessionState.catalog,
                  createOdpsTableAsSelectCommand.tableDesc,
                  tableExists = false)
                .asInstanceOf[InsertIntoOdpsTable],
              true
            )
          case _ =>
            throw new UnsupportedOperationException(
              s"Can't transform HiveTableScanExecTransformer from " +
                s"${dataWritingCommandExec.cmd.getClass.getSimpleName}")
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Can't transform HiveTableScanExecTransformer from ${plan.getClass.getSimpleName}")
    }
  }
}
