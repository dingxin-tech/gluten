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
package org.apache.gluten.extension.columnar

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.{LogLevelUtil, PlanUtil}

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.odps.OdpsTableInsertExecTransformer

/**
 * Converts a vanilla Spark plan node into Gluten plan node. Gluten plan is supposed to be executed
 * in native, and the internals of execution is subject by backend's implementation.
 *
 * Note: Only the current plan node is supposed to be open to modification. Do not access or modify
 * the children node. Tree-walking is done by caller of this trait.
 */
sealed trait OffloadSingleNode extends Logging {
  def offload(plan: SparkPlan): SparkPlan
}

// Aggregation transformation.
case class OffloadAggregate() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case plan if TransformHints.isNotTransformable(plan) =>
      plan
    case agg: HashAggregateExec =>
      genHashAggregateExec(agg)
    case other => other
  }

  /**
   * Generate a plan for hash aggregation.
   *
   * @param plan
   *   : the original Spark plan.
   * @return
   *   the actually used plan for execution.
   */
  private def genHashAggregateExec(plan: HashAggregateExec): SparkPlan = {
    if (TransformHints.isNotTransformable(plan)) {
      return plan
    }

    val aggChild = plan.child

    // If child's output is empty, fallback or offload both the child and aggregation.
    if (
      aggChild.output.isEmpty && BackendsApiManager.getSettings
        .fallbackAggregateWithEmptyOutputChild()
    ) {
      aggChild match {
        case _: TransformSupport =>
          // If the child is transformable, transform aggregation as well.
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)()
        case p: SparkPlan if PlanUtil.isGlutenTableCache(p) =>
          HashAggregateExecBaseTransformer.from(plan)()
        case _ =>
          // If the child is not transformable, do not transform the agg.
          TransformHints.tagNotTransformable(plan, "child output schema is empty")
          plan
      }
    } else {
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      HashAggregateExecBaseTransformer.from(plan)()
    }
  }
}

// Exchange transformation.
case class OffloadExchange() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case plan if TransformHints.isNotTransformable(plan) =>
      plan
    case plan: ShuffleExchangeExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      val child = plan.child
      if (
        (child.supportsColumnar || GlutenConfig.getConf.enablePreferColumnar) &&
        BackendsApiManager.getSettings.supportColumnarShuffleExec()
      ) {
        BackendsApiManager.getSparkPlanExecApiInstance.genColumnarShuffleExchange(plan, child)
      } else {
        plan.withNewChildren(Seq(child))
      }
    case plan: BroadcastExchangeExec =>
      val child = plan.child
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarBroadcastExchangeExec(plan.mode, child)
    case other => other
  }
}

// Join transformation.
case class OffloadJoin() extends OffloadSingleNode with LogLevelUtil {

  override def offload(plan: SparkPlan): SparkPlan = {
    if (TransformHints.isNotTransformable(plan)) {
      logDebug(s"Columnar Processing for ${plan.getClass} is under row guard.")
      return plan
    }
    plan match {
      case plan: ShuffledHashJoinExec =>
        val left = plan.left
        val right = plan.right
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genShuffledHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            TransformHints.getShuffleHashJoinBuildSide(plan),
            plan.condition,
            left,
            right,
            plan.isSkewJoin)
      case plan: SortMergeJoinExec =>
        val left = plan.left
        val right = plan.right
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genSortMergeJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.condition,
            left,
            right,
            plan.isSkewJoin)
      case plan: BroadcastHashJoinExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            left,
            right,
            isNullAwareAntiJoin = plan.isNullAwareAntiJoin)
      case plan: CartesianProductExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genCartesianProductExecTransformer(left, right, plan.condition)
      case plan: BroadcastNestedLoopJoinExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastNestedLoopJoinExecTransformer(
            left,
            right,
            plan.buildSide,
            plan.joinType,
            plan.condition)
      case other => other
    }
  }

}

// Filter transformation.
case class OffloadFilter() extends OffloadSingleNode with LogLevelUtil {
  import OffloadOthers._
  private val replace = new ReplaceSingleNode()

  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case filter: FilterExec =>
      genFilterExec(filter)
    case other => other
  }

  /**
   * Generate a plan for filter.
   *
   * @param filter
   *   : the original Spark plan.
   * @return
   *   the actually used plan for execution.
   */
  private def genFilterExec(filter: FilterExec): SparkPlan = {
    if (TransformHints.isNotTransformable(filter)) {
      return filter
    }

    // FIXME: Filter push-down should be better done by Vanilla Spark's planner or by
    //  a individual rule.
    // Push down the left conditions in Filter into FileSourceScan.
    val newChild: SparkPlan = filter.child match {
      case scan @ (_: FileSourceScanExec | _: BatchScanExec) =>
        if (TransformHints.isTransformable(scan)) {
          val newScan =
            FilterHandler.pushFilterToScan(filter.condition, scan)
          newScan match {
            case ts: TransformSupport if ts.doValidate().isValid => ts
            // TODO remove the call
            case _ => replace.doReplace(scan)
          }
        } else {
          replace.doReplace(scan)
        }
      case _ => replace.doReplace(filter.child)
    }
    logDebug(s"Columnar Processing for ${filter.getClass} is currently supported.")
    BackendsApiManager.getSparkPlanExecApiInstance
      .genFilterExecTransformer(filter.condition, newChild)
  }
}

// Other transformations.
case class OffloadOthers() extends OffloadSingleNode with LogLevelUtil {
  import OffloadOthers._
  private val replace = new ReplaceSingleNode()

  override def offload(plan: SparkPlan): SparkPlan = replace.doReplace(plan)
}

object OffloadOthers {
  // Utility to replace single node within transformed Gluten node.
  // Children will be preserved as they are as children of the output node.
  //
  // Do not look-up on children on the input node in this rule. Otherwise
  // it may break RAS which would group all the possible input nodes to
  // search for validate candidates.
  class ReplaceSingleNode() extends LogLevelUtil with Logging {

    def doReplace(p: SparkPlan): SparkPlan = {
      val plan = p
      if (TransformHints.isNotTransformable(plan)) {
        logDebug(s"Columnar Processing for ${plan.getClass} is under row guard.")
        plan match {
          case plan: BatchScanExec =>
            return applyScanNotTransformable(plan)
          case plan: FileSourceScanExec =>
            return applyScanNotTransformable(plan)
          case plan if HiveTableScanExecTransformer.isOdpsTableScan(plan) =>
            return applyScanNotTransformable(plan)
          case p =>
            return p
        }
      }
      plan match {
        case plan: BatchScanExec =>
          applyScanTransformer(plan)
        case plan: FileSourceScanExec =>
          applyScanTransformer(plan)
        case plan if HiveTableScanExecTransformer.isOdpsTableScan(plan) =>
          applyScanTransformer(plan)
        case plan: CoalesceExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceExecTransformer(plan.numPartitions, plan.child)
        case plan: ProjectExec =>
          val columnarChild = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ProjectExecTransformer(plan.projectList, columnarChild)
        case plan: SortAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan) {
            case sort: SortExecTransformer if !sort.global =>
              sort.child
            case sort: SortExec if !sort.global =>
              sort.child
            case other => other
          }
        case plan: ObjectHashAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)()
        case plan: UnionExec =>
          val children = plan.children
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ColumnarUnionExec(children)
        case plan: ExpandExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ExpandExecTransformer(plan.projections, plan.output, child)
        case plan if OdpsTableInsertExecTransformer.isOdpsTableWrite(plan) =>
          print(s"Columnar Processing for ${plan.getClass} is currently supported.")
          OdpsTableInsertExecTransformer.apply(plan)
        case plan: WriteFilesExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val writeTransformer = WriteFilesExecTransformer(
            child,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions)
          BackendsApiManager.getSparkPlanExecApiInstance.createColumnarWriteFilesExec(
            writeTransformer,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions
          )
        case plan: SortExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          SortExecTransformer(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
        case plan: TakeOrderedAndProjectExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
          TakeOrderedAndProjectExecTransformer(
            limit,
            plan.sortOrder,
            plan.projectList,
            child,
            offset)
        case plan: WindowExec =>
          WindowExecTransformer(
            plan.windowExpression,
            plan.partitionSpec,
            plan.orderSpec,
            plan.child)
        case plan if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(plan) =>
          val windowGroupLimitPlan = SparkShimLoader.getSparkShims
            .getWindowGroupLimitExecShim(plan)
            .asInstanceOf[WindowGroupLimitExecShim]
          WindowGroupLimitExecTransformer(
            windowGroupLimitPlan.partitionSpec,
            windowGroupLimitPlan.orderSpec,
            windowGroupLimitPlan.rankLikeFunction,
            windowGroupLimitPlan.limit,
            windowGroupLimitPlan.mode,
            windowGroupLimitPlan.child
          )
        case plan: GlobalLimitExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          val (limit, offset) =
            SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
          LimitTransformer(child, offset, limit)
        case plan: LocalLimitExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          LimitTransformer(child, 0L, plan.limit)
        case plan: GenerateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          BackendsApiManager.getSparkPlanExecApiInstance.genGenerateTransformer(
            plan.generator,
            plan.requiredChildOutput,
            plan.outer,
            plan.generatorOutput,
            child)
        case plan: BatchEvalPythonExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
        case plan: ArrowEvalPythonExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          // For ArrowEvalPythonExec, CH supports it through EvalPythonExecTransformer while
          // Velox backend uses ColumnarArrowEvalPythonExec.
          if (!BackendsApiManager.getSettings.supportColumnarArrowUdf()) {
            EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
          } else {
            BackendsApiManager.getSparkPlanExecApiInstance.createColumnarArrowEvalPythonExec(
              plan.udfs,
              plan.resultAttrs,
              child,
              plan.evalType)
          }
        case plan: SampleExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          BackendsApiManager.getSparkPlanExecApiInstance.genSampleExecTransformer(
            plan.lowerBound,
            plan.upperBound,
            plan.withReplacement,
            plan.seed,
            child)
        case p if !p.isInstanceOf[GlutenPlan] =>
          logDebug(s"Transformation for ${p.getClass} is currently not supported.")
          val children = plan.children
          p.withNewChildren(children)
        case other => other
      }
    }

    // Since https://github.com/apache/incubator-gluten/pull/2701
    private def applyScanNotTransformable(plan: SparkPlan): SparkPlan = plan match {
      case plan: FileSourceScanExec =>
        val newPartitionFilters =
          ExpressionConverter.transformDynamicPruningExpr(plan.partitionFilters)
        val newSource = plan.copy(partitionFilters = newPartitionFilters)
        if (plan.logicalLink.nonEmpty) {
          newSource.setLogicalLink(plan.logicalLink.get)
        }
        TransformHints.tag(newSource, TransformHints.getHint(plan))
        newSource
      case plan: BatchScanExec =>
        val newPartitionFilters: Seq[Expression] = plan.scan match {
          case scan: FileScan =>
            ExpressionConverter.transformDynamicPruningExpr(scan.partitionFilters)
          case _ =>
            ExpressionConverter.transformDynamicPruningExpr(plan.runtimeFilters)
        }
        val newSource = plan.copy(runtimeFilters = newPartitionFilters)
        if (plan.logicalLink.nonEmpty) {
          newSource.setLogicalLink(plan.logicalLink.get)
        }
        TransformHints.tag(newSource, TransformHints.getHint(plan))
        newSource
      case plan if HiveTableScanExecTransformer.isOdpsTableScan(plan) =>
        val newPartitionFilters: Seq[Expression] =
          ExpressionConverter.transformDynamicPruningExpr(
            HiveTableScanExecTransformer.getPartitionFilters(plan))
        val newSource = HiveTableScanExecTransformer.copyWith(plan, newPartitionFilters)
        if (plan.logicalLink.nonEmpty) {
          newSource.setLogicalLink(plan.logicalLink.get)
        }
        TransformHints.tag(newSource, TransformHints.getHint(plan))
        newSource
      case other =>
        throw new UnsupportedOperationException(s"${other.getClass.toString} is not supported.")
    }

    /**
     * Apply scan transformer for file source and batch source,
     *   1. create new filter and scan transformer, 2. validate, tag new scan as unsupported if
     *      failed, 3. return new source.
     */
    private def applyScanTransformer(plan: SparkPlan): SparkPlan = plan match {
      case plan: FileSourceScanExec =>
        val transformer = ScanTransformerFactory.createFileSourceScanTransformer(plan)
        val validationResult = transformer.doValidate()
        if (validationResult.isValid) {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          transformer
        } else {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
          val newSource = plan.copy(partitionFilters = transformer.getPartitionFilters())
          TransformHints.tagNotTransformable(newSource, validationResult.reason.get)
          newSource
        }
      case plan: BatchScanExec =>
        ScanTransformerFactory.createBatchScanTransformer(plan)

      case plan if HiveTableScanExecTransformer.isOdpsTableScan(plan) =>
        // TODO: Add DynamicPartitionPruningHiveScanSuite.scala
        val newPartitionFilters: Seq[Expression] =
          ExpressionConverter.transformDynamicPruningExpr(
            HiveTableScanExecTransformer.getPartitionFilters(plan))
        val hiveTableScanExecTransformer =
          BackendsApiManager.getSparkPlanExecApiInstance.genHiveTableScanExecTransformer(plan)
        val validateResult = hiveTableScanExecTransformer.doValidate()
        if (validateResult.isValid) {
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          return hiveTableScanExecTransformer
        }
        logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
        val newSource = HiveTableScanExecTransformer.copyWith(plan, newPartitionFilters)
        TransformHints.tagNotTransformable(newSource, validateResult.reason.get)
        newSource
      case other =>
        throw new GlutenNotSupportException(s"${other.getClass.toString} is not supported.")
    }
  }
}
