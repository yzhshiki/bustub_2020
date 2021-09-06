//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  limit_ = plan_->GetLimit();
  offset_ = plan_->GetOffset();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    if (offset_ != 0) {
      offset_--;
      break;
    }
    if (limit_ != 0) {
      offset_ = plan_->GetOffset();
      limit_--;
      return true;
    }
    break;
  }
  return false;
}

}  // namespace bustub
