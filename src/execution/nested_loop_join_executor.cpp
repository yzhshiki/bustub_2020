//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      outer_ended_(false) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_executor_->Next(&last_outer_tuple_, &last_outer_rid_);
  outer_ended_ = false;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!last_joined_tuples.empty()) {
    *tuple = last_joined_tuples.back();
    last_joined_tuples.pop_back();
    return true;
  }
  Tuple *l_tuple = &last_outer_tuple_;
  [[maybe_unused]] RID *l_rid = &last_outer_rid_;
  Tuple r_tuple;
  RID r_rid;
  const auto *left_schema = left_executor_->GetOutputSchema();
  uint32_t left_colcount = left_schema->GetColumnCount();
  const auto *right_schema = right_executor_->GetOutputSchema();
  uint32_t right_colcount = right_schema->GetColumnCount();
  while (!outer_ended_) {
    // std::cout<<"outer--- looping\n";
    while (right_executor_->Next(&r_tuple, &r_rid)) {
      // std::cout<<"inner looping\n";
      if (plan_->Predicate()->EvaluateJoin(l_tuple, left_schema, &r_tuple, right_schema).GetAs<bool>()) {
        std::vector<Value> values;
        for (uint32_t col_id = 0; col_id < left_colcount; col_id++) {
          values.push_back(l_tuple->GetValue(left_schema, col_id));
        }
        for (uint32_t col_id = 0; col_id < right_colcount; col_id++) {
          values.push_back(r_tuple.GetValue(right_schema, col_id));
        }
        Tuple out_tuple(values, plan_->OutputSchema());
        last_joined_tuples.push_back(out_tuple);
      }
    }
    // 此处，inner table已走完一遍，应outer iter++，并重置inner iter
    outer_ended_ = !left_executor_->Next(&last_outer_tuple_, &last_outer_rid_);
    right_executor_->Init();
  }
  if (!last_joined_tuples.empty()) {
    *tuple = last_joined_tuples.back();
    last_joined_tuples.pop_back();
    return true;
  }
  return false;
}

}  // namespace bustub
