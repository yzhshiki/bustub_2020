//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  // 首先把所有子节点的tuple中的所需数据取出，放入哈希表
  bool aht_changed = false;
  while (child_->Next(tuple, rid)) {
    aht_.InsertCombine(MakeKey(tuple), MakeVal(tuple));
    aht_changed = true;
    // std::cout<<"aggregation inserting combine\n";
  }
  if (aht_changed) {
    aht_iterator_ = aht_.Begin();
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  if (plan_->GetHaving() != nullptr) {
    while (plan_->GetHaving()
               ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
               .GetAs<bool>()) {
      Tuple result(aht_iterator_.Val().aggregates_, plan_->OutputSchema());
      *tuple = result;
      ++aht_iterator_;
      return true;
    }
  } else {
    Tuple result(aht_iterator_.Val().aggregates_, plan_->OutputSchema());
    *tuple = result;
    ++aht_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub
