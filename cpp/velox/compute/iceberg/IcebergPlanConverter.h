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

#pragma once

#include "substrait/SubstraitToVeloxPlan.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"

using namespace facebook::velox::connector::hive::iceberg;

namespace gluten {
struct IcebergSplitInfo : SplitInfo {
  std::vector<std::vector<IcebergDeleteFile>> deleteFilesVec;
  std::vector<std::string> paths;
  dwio::common::FileFormat format;

  IcebergSplitInfo(const SplitInfo& splitInfo) : SplitInfo(splitInfo) {
    // Reserve the actual size of the deleteFilesVec.
    deleteFilesVec.reserve(0);
  }
};

class IcebergPlanConverter {
 public:
  static std::shared_ptr<IcebergSplitInfo> parseIcebergSplitInfo(
      substrait::ReadRel_LocalFiles_FileOrFiles file,
      std::shared_ptr<SplitInfo> splitInfo);
};

} // namespace gluten
