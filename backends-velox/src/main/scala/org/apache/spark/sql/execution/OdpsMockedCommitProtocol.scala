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

import org.apache.spark.TaskContext
import org.apache.spark.internal.io.SparkHadoopWriterUtils.createJobID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

/** @author dingxin (zhangdingxin.zdx@alibaba-inc.com) */
class OdpsMockedCommitProtocol(jobTrackerID: String) {

  val sparkStageId = TaskContext.get().stageId()
  val sparkPartitionId = TaskContext.get().partitionId()
  val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue
  private val jobId = createJobID(jobTrackerID, sparkStageId)

  private val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)

  val taskAttemptId = new TaskAttemptID(taskId, 1)

  val taskAttemptContext: TaskAttemptContext = {
    new TaskAttemptContextImpl(new Configuration(), taskAttemptId)
  }
  def newTaskAttemptTempPath() = {
    print("newTaskAttemptTempPath")
    "path"
  }

  def setupTask() = {
    print("setupTask")
  }

  def commitTask(): Unit = {
    print("commitTask")
  }

  def abortTask(): Unit = {
    print("abortTask")
  }
  def getJobId: String = jobId.toString

}