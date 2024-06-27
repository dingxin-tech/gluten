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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.odps.OdpsClient

import com.aliyun.odps.table.TableIdentifier
import com.aliyun.odps.table.configuration.ArrowOptions
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.write.{TableWriteCapabilities, TableWriteSessionBuilder}

class OdpsMockedCommitProtocol(table: CatalogTable, partition: Map[String, Option[String]])
  extends Logging {

  def newTaskAttemptTempPath(): String = {
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
    table.identifier.toString()
  }

  def setupTask(): Unit = {
    print("setupTask")
  }

  def commitTask(): Unit = {
    print("commitTask")
  }

  def abortTask(): Unit = {
    print("abortTask")
  }
  def getJobId: String = table.identifier.toString()
}
