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
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.InsertIntoOdpsTable
import org.apache.spark.sql.types._

import com.aliyun.odps.PartitionSpec
import org.apache.hadoop.hive.ql.ErrorMsg

import java.lang

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`;

/** @author dingxin (zhangdingxin.zdx@alibaba-inc.com) */
@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
case class OdpsTableInsertExecTransformer(
    child: SparkPlan,
    insertIntoOdpsTable: InsertIntoOdpsTable)
  extends UnaryTransformSupport {

  val table: CatalogTable = insertIntoOdpsTable.table
  val partition: Map[String, Option[String]] = insertIntoOdpsTable.partition
  val query: LogicalPlan = insertIntoOdpsTable.query
  val overwrite: Boolean = insertIntoOdpsTable.overwrite
  val ifPartitionNotExists: Boolean = insertIntoOdpsTable.ifPartitionNotExists
  val outputColumnNames: Seq[String] = insertIntoOdpsTable.outputColumnNames

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

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val odpsPartitionSpec = new PartitionSpec

    val partitionSchema = table.partitionSchema
    val partitionColumnNames = table.partitionColumnNames

    // By this time, the partition map must match the table's partition columns
    if (partitionColumnNames.toSet != partition.keySet) {
      throw QueryExecutionErrors.requestedPartitionsMismatchTablePartitionsError(table, partition)
    }

    val outputPartitionColumns =
      insertIntoOdpsTable.outputColumns.filter(c => partitionSchema.getFieldIndex(c.name).isDefined)
    val outputPartitionSet = AttributeSet(outputPartitionColumns)
    val dataColumns = insertIntoOdpsTable.outputColumns.filterNot(outputPartitionSet.contains)

    if (partitionSchema.nonEmpty) {
      // val partitionSpec = partition.filter(_._2.nonEmpty).map { case (k, v) => k -> v.get }
      val partitionSpec = partition.map {
        // TODO: null partition
        case (key, Some(value)) => key -> value
        case (key, None) => key -> ""
      }

      // Validate partition spec if there exist any dynamic partitions
      if (numDynamicPartitions > 0) {
        // Report error if any static partition appears after a dynamic partition
        val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
        if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
          throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
        }

        val dynamicPartitionSchema = StructType(partitionSchema.takeRight(numDynamicPartitions))
        dynamicPartitionSchema.map(_.dataType).foreach {
          case StringType | LongType | IntegerType | ShortType | ByteType =>
          case dt: DataType =>
            throw new SparkException(s"Unsupported partition column type: ${dt.simpleString}")
        }
      }

      if (numStaticPartitions > 0) {
        var part = 0
        partitionColumnNames.foreach {
          field =>
            if (part < numStaticPartitions) {
              odpsPartitionSpec.set(field, partitionSpec(field))
              part = part + 1
            }
        }
      }

      val isStaticPartition = numStaticPartitions > 0 && numDynamicPartitions == 0
    }

    val project = table.database
    val tableName = table.identifier.table

    print(project)
    print(tableName)
    print(odpsPartitionSpec)

    print(partitionSchema)
    print(partitionColumnNames)

    val columnTypeNodes = new java.util.ArrayList[ColumnTypeNode]()
    val inputAttributes = new java.util.ArrayList[Attribute]()
    val childSize = this.child.output.size
    val childOutput = this.child.output
    for (i <- 0 until childSize) {
      columnTypeNodes.add(new ColumnTypeNode(0))
      inputAttributes.add(attributes(i))
    }

    val typeNodes = ConverterUtils.collectAttributeTypeNodes(attributes)

    val nameList =
      ConverterUtils.collectAttributeNames(inputAttributes.toSeq)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(createEnhancement(attributes))

    RelBuilder.makeWriteRel(
      input,
      typeNodes,
      nameList,
      columnTypeNodes,
      extensionNode,
      context,
      operatorId)
  }

  def createEnhancement(output: Seq[Attribute]): com.google.protobuf.Any = {
    val inputTypeNodes = output.map {
      attr => {
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
        if (dataWritingCommandExec.cmd.isInstanceOf[InsertIntoOdpsTable]) {
          print("is InsertIntoOdpsTable")
          ValidationResult.ok
        } else {
          ValidationResult.notOk("Is not InsertIntoOdpsTable")
        }
      case _ => ValidationResult.notOk("Is not InsertIntoOdpsTable")
    }
  }

  def apply(plan: SparkPlan): OdpsTableInsertExecTransformer = {
    plan match {
      case dataWritingCommandExec: DataWritingCommandExec =>
        val insertIntoOdpsTable = dataWritingCommandExec.cmd.asInstanceOf[InsertIntoOdpsTable]

        new OdpsTableInsertExecTransformer(
          dataWritingCommandExec.child,
          insertIntoOdpsTable
        )
      case _ =>
        throw new UnsupportedOperationException(
          s"Can't transform HiveTableScanExecTransformer from ${plan.getClass.getSimpleName}")
    }
  }
}
