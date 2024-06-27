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
import org.apache.spark.sql.catalyst.expressions.{Ascending, BindReferences, SortOrder}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.InsertIntoOdpsTable

object RewriteInsertIntoOdps extends RewriteSingleNode with Logging {
  override def rewrite(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      case insertPlan: DataWritingCommandExec =>
        val insertIntoOdpsTable = insertPlan.cmd.asInstanceOf[InsertIntoOdpsTable]

        val partitionSchema = insertIntoOdpsTable.table.partitionSchema
        val outputPartitionColumns =
          insertIntoOdpsTable.outputColumns.filter(c => partitionSchema.fieldNames.contains(c.name))
        val dynamicPartitionColumns = outputPartitionColumns

        val outputColumns = insertPlan.output

        // We should first sort by partition columns, then bucket id, and finally sorting columns.
        val requiredOrdering = dynamicPartitionColumns
        // the sort order doesn't matter
        val actualOrdering = plan.outputOrdering.map(_.child)
        logInfo(s"requiredOrdering: $requiredOrdering, actualOrdering: $actualOrdering")

        val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
          false
        } else {
          requiredOrdering.zip(actualOrdering).forall {
            case (requiredOrder, childOutputOrder) =>
              requiredOrder.semanticEquals(childOutputOrder)
          }
        }

        if (orderingMatched) {
          plan
        } else {
          val orderingExpr = requiredOrdering
            .map(SortOrder(_, Ascending))
            .map(BindReferences.bindReference(_, outputColumns))
          SortExec(orderingExpr, global = false, child = plan)
        }
      case _ => plan
    }
  }
}
