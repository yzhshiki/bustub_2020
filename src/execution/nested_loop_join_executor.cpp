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
    last_outer_tuple_(),
    last_outer_rid_(),
    outer_ended_(false) {}

void NestedLoopJoinExecutor::Init() {
    if(left_executor_ != nullptr) {
        left_executor_->Init();
    }
    if(right_executor_ != nullptr) {
        right_executor_->Init();
    }
    left_executor_->Next(&last_outer_tuple_, &last_outer_rid_);
    outer_ended_ = false;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
    Tuple *l_tuple = &last_outer_tuple_;
    [[maybe_unused]]RID *l_rid = &last_outer_rid_;
    Tuple r_tuple;
    RID r_rid;
    while(!outer_ended_) {
        // std::cout<<"outer--- looping\n";
        while(right_executor_->Next(&r_tuple, &r_rid)) {
            // std::cout<<"inner looping\n";
            if(plan_->Predicate()->EvaluateJoin(l_tuple, left_executor_->GetOutputSchema(), &r_tuple, right_executor_->GetOutputSchema()).GetAs<bool>()) {
                std::vector<Value> values;
                for(uint32_t col_id = 0; col_id < left_executor_->GetOutputSchema()->GetColumnCount(); col_id++) {
                    values.push_back(l_tuple->GetValue(left_executor_->GetOutputSchema(), col_id));
                }
                for(uint32_t col_id = 0; col_id < right_executor_->GetOutputSchema()->GetColumnCount(); col_id++) {
                    values.push_back(r_tuple.GetValue(right_executor_->GetOutputSchema(), col_id));
                }
                Tuple out_tuple(values, plan_->OutputSchema());
                *tuple = out_tuple;
                return true;
            }
        }
        // 此处，inner table已走完一遍，应outer iter++，并重置inner iter
        outer_ended_ = !left_executor_->Next(&last_outer_tuple_, &last_outer_rid_);
        right_executor_->Init();
    }
    return false;
}

}  // namespace bustub
