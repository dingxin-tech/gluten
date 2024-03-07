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

#include "SubstraitToVeloxExpr.h"
#include "TypeUtils.h"
#include "velox/connectors/odps/OdpsConnector.h"
#include "velox/connectors/odps/TableHandle.hpp"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"
#include <string>

namespace gluten {

struct SplitInfo {
  /// Whether the split comes from arrow array stream node.
  bool isStream = false;

  string projectName;
  string schemaName;
  string tableName;

  string sessionId;

  int index;
};

/// This class is used to convert the Substrait plan into Velox plan.
class SubstraitToVeloxPlanConverter {
 public:
  SubstraitToVeloxPlanConverter(
      memory::MemoryPool* pool,
      const std::unordered_map<std::string, std::string>& confMap = {},
      bool validationMode = false)
      : pool_(pool), confMap_(confMap), validationMode_(validationMode) {}

  /// Used to convert Substrait ExpandRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ExpandRel& expandRel);

  /// Used to convert Substrait GenerateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::GenerateRel& generateRel);

  /// Used to convert Substrait SortRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WindowRel& windowRel);

  /// Used to convert Substrait JoinRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::JoinRel& joinRel);

  /// Used to convert Substrait AggregateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::AggregateRel& aggRel);

  /// Convert Substrait ProjectRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ProjectRel& projectRel);

  /// Convert Substrait FilterRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::FilterRel& filterRel);

  /// Convert Substrait FetchRel into Velox LimitNode or TopNNode according the
  /// different input of fetchRel.
  core::PlanNodePtr toVeloxPlan(const ::substrait::FetchRel& fetchRel);

  /// Convert Substrait ReadRel into Velox Values Node.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ReadRel& readRel, const RowTypePtr& type);

  /// Convert Substrait SortRel into Velox OrderByNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::SortRel& sortRel);

  /// Convert Substrait ReadRel into Velox PlanNode.
  /// Index: the index of the partition this item belongs to.
  /// Starts: the start positions in byte to read from the items.
  /// Lengths: the lengths in byte to read from the items.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ReadRel& sRead);

  /// Used to convert Substrait Rel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::Rel& sRel);

  /// Used to convert Substrait RelRoot into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::RelRoot& sRoot);

  /// Used to convert Substrait Plan into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::Plan& substraitPlan);

  // return the raw ptr of ExprConverter
  SubstraitVeloxExprConverter* getExprConverter() {
    return exprConverter_.get();
  }

  /// Used to construct the function map between the index
  /// and the Substrait function name. Initialize the expression
  /// converter based on the constructed function map.
  void constructFunctionMap(const ::substrait::Plan& substraitPlan);

  /// Will return the function map used by this plan converter.
  const std::unordered_map<uint64_t, std::string>& getFunctionMap() const {
    return functionMap_;
  }

  /// Return the splitInfo map used by this plan converter.
  const std::unordered_map<core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfos() const {
    return splitInfoMap_;
  }

  /// Used to insert certain plan node as input. The plan node
  /// id will start from the setted one.
  void insertInputNode(uint64_t inputIdx, const std::shared_ptr<const core::PlanNode>& inputNode, int planNodeId) {
    inputNodesMap_[inputIdx] = inputNode;
    planNodeId_ = planNodeId;
  }

  /// Used to check if ReadRel specifies an input of stream.
  /// If yes, the index of input stream will be returned.
  /// If not, -1 will be returned.
  int32_t getStreamIndex(const ::substrait::ReadRel& sRel);

  /// Used to find the function specification in the constructed function map.
  std::string findFuncSpec(uint64_t id);

  /// Extract join keys from joinExpression.
  /// joinExpression is a boolean condition that describes whether each record
  /// from the left set “match” the record from the right set. The condition
  /// must only include the following operations: AND, ==, field references.
  /// Field references correspond to the direct output order of the data.
  void extractJoinKeys(
      const ::substrait::Expression& joinExpression,
      std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
      std::vector<const ::substrait::Expression::FieldReference*>& rightExprs);

  /// Get aggregation step from AggregateRel.
  core::AggregationNode::Step toAggregationStep(const ::substrait::AggregateRel& sAgg);

  /// Helper Function to convert Substrait sortField to Velox sortingKeys and
  /// sortingOrders.
  std::pair<std::vector<core::FieldAccessTypedExprPtr>, std::vector<core::SortOrder>> processSortField(
      const ::google::protobuf::RepeatedPtrField<::substrait::SortField>& sortField,
      const RowTypePtr& inputType);

 private:
  /// Integrate Substrait emit feature. Here a given 'substrait::RelCommon'
  /// is passed and check if emit is defined for this relation. Basically a
  /// ProjectNode is added on top of 'noEmitNode' to represent output order
  /// specified in 'relCommon::emit'. Return 'noEmitNode' as is
  /// if output order is 'kDriect'.
  core::PlanNodePtr processEmit(const ::substrait::RelCommon& relCommon, const core::PlanNodePtr& noEmitNode);

  /// Multiple conditions are connected to a binary tree structure with
  /// the relation key words, including AND, OR, and etc. Currently, only
  /// AND is supported. This function is used to extract all the Substrait
  /// conditions in the binary tree structure into a vector.
  void flattenConditions(
      const ::substrait::Expression& sFilter,
      std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions,
      std::vector<::substrait::Expression_SingularOrList>& singularOrLists,
      std::vector<::substrait::Expression_IfThen>& ifThens);

  /// Check the Substrait type extension only has one unknown extension.
  static bool checkTypeExtension(const ::substrait::Plan& substraitPlan);

  /// Returns unique ID to use for plan node. Produces sequential numbers
  /// starting from zero.
  std::string nextPlanNodeId();

  /// Returns whether the args of a scalar function being field or
  /// field with literal. If yes, extract and set the field index.
  static bool fieldOrWithLiteral(
      const ::google::protobuf::RepeatedPtrField<::substrait::FunctionArgument>& arguments,
      uint32_t& fieldIndex);

  /// Returns whether a function can be pushed down.
  static bool canPushdownCommonFunction(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      const std::string& filterName,
      uint32_t& fieldIdx);

  /// Returns whether a NOT function can be pushed down.
  bool canPushdownNot(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      std::vector<RangeRecorder>& rangeRecorders);

  /// Returns whether a OR function can be pushed down.
  bool canPushdownOr(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      std::vector<RangeRecorder>& rangeRecorders);

  /// Returns whether a SingularOrList can be pushed down.
  static bool canPushdownSingularOrList(
      const ::substrait::Expression_SingularOrList& singularOrList,
      bool disableIntLike = false);

  /// Check whether the children functions of this scalar function have the same
  /// column index. Curretly used to check whether the two chilren functions of
  /// 'or' expression are effective on the same column.
  static bool childrenFunctionsOnSameField(const ::substrait::Expression_ScalarFunction& function);

  /// Extract SingularOrList and returns the field index.
  static uint32_t getColumnIndexFromSingularOrList(const ::substrait::Expression_SingularOrList&);


  /// Create a multirange to specify the filter 'x != notValue' with:
  /// x > notValue or x < notValue.
  template <TypeKind KIND, typename FilterType>
  void createNotEqualFilter(variant notVariant, bool nullAllowed, std::vector<std::unique_ptr<FilterType>>& colFilters);


  /// Connect all remaining functions with 'and' relation
  /// for the use of remaingFilter in Odps Connector.
  core::TypedExprPtr connectWithAnd(
      std::vector<std::string> inputNameList,
      std::vector<TypePtr> inputTypeList,
      const std::vector<::substrait::Expression_ScalarFunction>& remainingFunctions,
      const std::vector<::substrait::Expression_SingularOrList>& singularOrLists,
      const std::vector<::substrait::Expression_IfThen>& ifThens);

  /// Connect the left and right expressions with 'and' relation.
  core::TypedExprPtr connectWithAnd(core::TypedExprPtr leftExpr, core::TypedExprPtr rightExpr);

  /// Used to convert AggregateRel into Velox plan node.
  /// The output of child node will be used as the input of Aggregation.
  std::shared_ptr<const core::PlanNode> toVeloxAgg(
      const ::substrait::AggregateRel& sAgg,
      const std::shared_ptr<const core::PlanNode>& childNode,
      const core::AggregationNode::Step& aggStep);

  /// Helper function to convert the input of Substrait Rel to Velox Node.
  template <typename T>
  core::PlanNodePtr convertSingleInput(T rel) {
    VELOX_CHECK(rel.has_input(), "Child Rel is expected here.");
    return toVeloxPlan(rel.input());
  }

  /// The unique identification for each PlanNode.
  int planNodeId_ = 0;

  /// The map storing the relations between the function id and the function
  /// name. Will be constructed based on the Substrait representation.
  std::unordered_map<uint64_t, std::string> functionMap_;

  /// The map storing the split stats for each PlanNode.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<SplitInfo>> splitInfoMap_;

  /// The map storing the pre-built plan nodes which can be accessed through
  /// index. This map is only used when the computation of a Substrait plan
  /// depends on other input nodes.
  std::unordered_map<uint64_t, std::shared_ptr<const core::PlanNode>> inputNodesMap_;

  /// The Expression converter used to convert Substrait representations into
  /// Velox expressions.
  std::unique_ptr<SubstraitVeloxExprConverter> exprConverter_;

  /// Memory pool.
  memory::MemoryPool* pool_;

  /// A map of custom configs.
  std::unordered_map<std::string, std::string> confMap_;

  /// A flag used to specify validation.
  bool validationMode_ = false;
};

} // namespace gluten
