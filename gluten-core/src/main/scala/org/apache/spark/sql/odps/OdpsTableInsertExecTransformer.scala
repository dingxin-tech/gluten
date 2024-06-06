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
import org.apache.gluten.execution.UnaryTransformSupport
import org.apache.gluten.metrics.MetricsUpdater

import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan;

/** @author dingxin (zhangdingxin.zdx@alibaba-inc.com) */
class OdpsTableInsertExecTransformer(
    child: SparkPlan,
    partitionColumns: Seq[Attribute],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends UnaryTransformSupport {
  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWriteFilesTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genWriteFilesTransformerMetricsUpdater(metrics)

  override def output: Seq[Attribute] = Seq.empty

  override protected def withNewChildInternal(
      newChild: SparkPlan): OdpsTableInsertExecTransformer = {
    null
  }

  override def productElement(n: Int): Any = null

  override def productArity: Int = null

  override def canEqual(that: Any): Boolean = false

  override def child: SparkPlan = null
}

object OdpsTableInsertExecTransformer {
  def isOdpsTableWrite(plan: SparkPlan): Boolean = {
    print(s"isOdpsTableScan: ${plan.getClass.getName}")
    false
  }
}
