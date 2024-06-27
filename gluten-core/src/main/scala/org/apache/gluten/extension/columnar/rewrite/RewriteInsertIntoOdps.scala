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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Ascending, BindReferences, SortOrder}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.{CreateOdpsTableAsSelectCommand, InsertIntoOdpsTable}

object RewriteInsertIntoOdps extends RewriteSingleNode with Logging {

  override def rewrite(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      case insertPlan: DataWritingCommandExec => handleDataWritingCommandExec(insertPlan)
      case _ => plan
    }
  }

  private def handleDataWritingCommandExec(insertPlan: DataWritingCommandExec): SparkPlan = {
    insertPlan.cmd match {
      case insertIntoOdpsTable: InsertIntoOdpsTable =>
        handleInsertIntoOdps(insertIntoOdpsTable, insertPlan)
      case createTableAsSelect: CreateOdpsTableAsSelectCommand =>
        val insertIntoOdpsTable = getInsertIntoOdpsTableFromCreateTable(createTableAsSelect)
        handleInsertIntoOdps(insertIntoOdpsTable, insertPlan)
      case _ => insertPlan
    }
  }

  private def getInsertIntoOdpsTableFromCreateTable(
      createTableAsSelect: CreateOdpsTableAsSelectCommand): InsertIntoOdpsTable = {
    val sparkSession: SparkSession = SparkSession.active
    val catalog = sparkSession.sessionState.catalog
    createTableAsSelect
      .getWritingCommand(catalog, createTableAsSelect.tableDesc, false)
      .asInstanceOf[InsertIntoOdpsTable]
  }

  private def handleInsertIntoOdps(
      insertIntoOdpsTable: InsertIntoOdpsTable,
      insertPlan: DataWritingCommandExec): SparkPlan = {

    val partitionSchema = insertIntoOdpsTable.table.partitionSchema
    val outputPartitionColumns =
      insertIntoOdpsTable.outputColumns.filter(c => partitionSchema.fieldNames.contains(c.name))
    val dynamicPartitionColumns = outputPartitionColumns
    val outputColumns = insertPlan.output

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val partitionOrderingColumns = dynamicPartitionColumns
    val actualOrdering = insertPlan.outputOrdering.map(_.child)

    // Logging required and actual ordering
    logInfo(s"requiredOrdering: $partitionOrderingColumns, actualOrdering: $actualOrdering")

    // Check if ordering matches
    val orderingMatched = partitionOrderingColumns.length <= actualOrdering.length &&
      partitionOrderingColumns.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) => requiredOrder.semanticEquals(childOutputOrder)
      }

    // If ordering matches, return original plan, otherwise add a SortExec
    if (orderingMatched) {
      insertPlan
    } else {
      val orderingExpr = partitionOrderingColumns
        .map(SortOrder(_, Ascending))
        .map(BindReferences.bindReference(_, outputColumns))
      SortExec(orderingExpr, global = false, child = insertPlan)
    }
  }
}
