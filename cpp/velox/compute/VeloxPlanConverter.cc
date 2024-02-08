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

#include "VeloxPlanConverter.h"
#include <filesystem>

#include "compute/ResultIterator.h"
#include "config/GlutenConfig.h"
#include "operators/plannodes/RowVectorStream.h"
#include "velox/common/file/FileSystems.h"

namespace gluten {

using namespace facebook;

VeloxPlanConverter::VeloxPlanConverter(
    const std::vector<std::shared_ptr<ResultIterator>>& inputIters,
    velox::memory::MemoryPool* veloxPool,
    const std::unordered_map<std::string, std::string>& confMap,
    const std::optional<std::string> writeFilesTempPath,
    bool validationMode)
    : validationMode_(validationMode),
      substraitVeloxPlanConverter_(veloxPool, confMap, writeFilesTempPath, validationMode),
      pool_(veloxPool) {
  // avoid include RowVectorStream.h in SubstraitToVeloxPlan.cpp, it may cause redefinition of array abi.h.
  auto factory = [inputIters = std::move(inputIters), validationMode = validationMode](
                     std::string nodeId, memory::MemoryPool* pool, int32_t streamIdx, RowTypePtr outputType) {
    std::shared_ptr<ResultIterator> iterator;
    if (!validationMode) {
      VELOX_CHECK_LT(streamIdx, inputIters.size(), "Could not find stream index {} in input iterator list.", streamIdx);
      iterator = inputIters[streamIdx];
    }
    auto valueStream = std::make_shared<RowVectorStream>(pool, iterator, outputType);
    return std::make_shared<ValueStreamNode>(nodeId, outputType, std::move(valueStream));
  };
  substraitVeloxPlanConverter_.setValueStreamNodeFactory(std::move(factory));
}

namespace {
void parseLocalFileNodes(
    SubstraitToVeloxPlanConverter* planConverter,
    std::vector<::substrait::ReadRel_ExtensionTable>& localFiles) {
  std::vector<std::shared_ptr<SplitInfo>> splitInfos;
  splitInfos.reserve(localFiles.size());
  for (int32_t i = 0; i < localFiles.size(); i++) {
    const auto& extensionTable = localFiles[i];
    if (extensionTable.detail().Is<::substrait::ReadRel_OdpsScanSplit>()) {
      ::substrait::ReadRel_OdpsScanSplit odpsScanSplit;
      if (extensionTable.detail().UnpackTo(&odpsScanSplit)) {
        auto splitInfo = std::make_shared<SplitInfo>();
        splitInfo->projectName = odpsScanSplit.project();
        splitInfo->schemaName = odpsScanSplit.schema();
        splitInfo->tableName = odpsScanSplit.table();
        splitInfo->sessionId = odpsScanSplit.sessionid();
        splitInfo->index = odpsScanSplit.index();
        splitInfo->row_index = odpsScanSplit.startindex();
        splitInfo->row_count = odpsScanSplit.numrecord();
        splitInfos.push_back(std::move(splitInfo));
      } else {
        std::cerr << "Error unpacking ExtensionTable detail as OdpsScanSplit." << std::endl;
      }
    } else {
      std::cerr << "ExtensionTable detail is not of type OdpsScanSplit." << std::endl;
    }
  }
  planConverter->setSplitInfos(std::move(splitInfos));
}
} // namespace

std::shared_ptr<const facebook::velox::core::PlanNode> VeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& substraitPlan,
    std::vector<::substrait::ReadRel_ExtensionTable> localFiles) {
  if (!validationMode_) {
    parseLocalFileNodes(&substraitVeloxPlanConverter_, localFiles);
  }

  auto veloxPlan = substraitVeloxPlanConverter_.toVeloxPlan(substraitPlan);
  DLOG(INFO) << "Plan Node: " << std::endl << veloxPlan->toString(true, true);
  return veloxPlan;
}

std::string VeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

} // namespace gluten
