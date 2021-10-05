//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_metadata_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      iter_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())),
      iter_end_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->End()),
      predicate_(plan->GetPredicate()),
      txn_manager_(exec_ctx_->GetTransactionManager()),
      txn_(exec_ctx_->GetTransaction()),
      lock_manager_(exec_ctx_->GetLockManager()) {}

void SeqScanExecutor::Init() {
  iter_ = table_metadata_->table_->Begin(exec_ctx_->GetTransaction());
  iter_end_ = table_metadata_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != iter_end_) {
    if(lock_manager_!= nullptr && txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if(!txn_->IsExclusiveLocked(iter_->GetRid()) && !txn_->IsSharedLocked(iter_->GetRid())) {
            try {
                bool locked = lock_manager_->LockShared(txn_, iter_->GetRid());
                if (!locked) {
                    txn_->SetState(TransactionState::ABORTED);
                    return false;
                }
            }
            catch (TransactionAbortException &e) {
                std::cout <<"ready to scan "<< e.GetInfo();
            }
        }
    }
    *rid = iter_->GetRid();
    Tuple tmp_tuple;
    tmp_tuple = *iter_;
    ++iter_;
    if (nullptr == predicate_ || predicate_->Evaluate(&tmp_tuple, &table_metadata_->schema_).GetAs<bool>()) {
      const Schema *output_schema = plan_->OutputSchema();
      std::vector<Value> values(output_schema->GetColumnCount());
      auto &output_column = output_schema->GetColumns();
      for (size_t i = 0; i < values.size(); ++i) {
        values[i] = output_column[i].GetExpr()->Evaluate(&tmp_tuple, &table_metadata_->schema_);
      }
      *tuple = Tuple(values, output_schema);
      // 可能要解锁
      if(lock_manager_ != nullptr && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
          bool unlocked = lock_manager_->Unlock(txn_, *rid);
          if(!unlocked) {
              txn_->SetState((TransactionState::ABORTED));
              std::cout<<"unlock failed\n";
              return false;
          }
          std::cout<<txn_->GetTransactionId()<<" unlocked LS\n";
      }
      return true;
    }
    // 可能要解锁
    if(lock_manager_ != nullptr && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      bool unlocked = lock_manager_->Unlock(txn_, *rid);
      if(!unlocked) {
          txn_->SetState((TransactionState::ABORTED));
          std::cout<<"unlock failed\n";
          return false;
      }
    }
  }
  return false;
}

}  // namespace bustub
