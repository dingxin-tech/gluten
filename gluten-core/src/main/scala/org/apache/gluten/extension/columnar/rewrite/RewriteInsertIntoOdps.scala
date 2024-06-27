package org.apache.gluten.extension.columnar.rewrite
import org.apache.spark.sql.catalyst.expressions.{Ascending, BindReferences, SortOrder}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.InsertIntoOdpsTable

object RewriteInsertIntoOdps extends RewriteSingleNode {
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
