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
package org.apache.gluten.substrait.rel;

import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.google.protobuf.Any;
import io.substrait.proto.ReadRel;
import io.substrait.proto.ReadRel.ExtensionTable;
import io.substrait.proto.ReadRel.ExtensionTable.Builder;
import io.substrait.proto.ReadRel.OdpsScanSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/** @author dingxin (zhangdingxin.zdx@alibaba-inc.com) */
public class OdpsScanNode implements SplitInfo {
  private static final Logger LOG = LoggerFactory.getLogger(OdpsScanNode.class);

  public static final OdpsScanNode EMPTY =
      new OdpsScanNode(TableIdentifier.of("project", "table"), new IndexedInputSplit("EMPTY", 0));
  private String projectName;
  private String schemaName;
  private String tableName;
  private String sessionId;
  private int index;
  private long startIndex;
  private long numRecord;

  public OdpsScanNode(TableIdentifier identifier, InputSplit inputSplit) {
    projectName = identifier.getProject();
    schemaName = identifier.getSchema();
    tableName = identifier.getTable();
    sessionId = inputSplit.getSessionId();
    if (inputSplit instanceof IndexedInputSplit) {
      index = ((IndexedInputSplit) inputSplit).getSplitIndex();
      LOG.info("sessionId: {}, index: {}", sessionId, index);
    } else if (inputSplit instanceof RowRangeInputSplit) {
      startIndex = ((RowRangeInputSplit) inputSplit).getRowRange().getStartIndex();
      numRecord = ((RowRangeInputSplit) inputSplit).getRowRange().getNumRecord();
      LOG.info("sessionId: {}, startIndex: {}, numRecord: {}", sessionId, startIndex, numRecord);
    }
  }

  public OdpsScanNode(long count) {
    sessionId = "COUNT";
    projectName = "";
    schemaName = "";
    tableName = "";
    index = 0;
    startIndex = 0;
    numRecord = count;
  }

  @Override
  public List<String> preferredLocations() {
    return Collections.emptyList();
  }

  @Override
  public ReadRel.ExtensionTable toProtobuf() {
    OdpsScanSplit odpsScanSplit =
        OdpsScanSplit.newBuilder()
            .setSessionId(sessionId)
            .setProject(projectName)
            .setSchema(schemaName)
            .setTable(tableName)
            .setIndex(index)
            .setStartIndex(startIndex)
            .setNumRecord(numRecord)
            .build();
    Builder builder = ExtensionTable.newBuilder();
    builder.setDetail(Any.pack(odpsScanSplit));
    return builder.build();
  }
}