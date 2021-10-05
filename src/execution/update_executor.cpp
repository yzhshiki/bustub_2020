//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx_->GetTransaction()),
      lock_manager_(exec_ctx_->GetLockManager()) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple Old_tuple;
  Tuple New_tuple;
  Transaction *txn = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&Old_tuple, rid)) {
      // 拿锁
    if(lock_manager_ != nullptr && txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try{
            bool locked = true;
//            if(!txn_->IsExclusiveLocked(*rid) && !txn_->IsSharedLocked(*rid)) {
//                std::cout<<txn_->GetTransactionId()<<" is asking LE\n";
//                locked = lock_manager_->LockExclusive(txn_, *rid);
//                std::cout<<"Got exclusive lock\n";
//            }
            if(!txn_->IsExclusiveLocked(*rid) && txn_->IsSharedLocked(*rid)) {
                locked = lock_manager_->LockUpgrade(txn_, *rid);
            }
            if(!locked) {
                txn_->SetState(TransactionState::ABORTED);
                return false;
            }
        }
        catch (TransactionAbortException &e) {
            std::cout<<"ready to update "<<e.GetInfo();
        }
    }
    New_tuple = GenerateUpdatedTuple(Old_tuple);
    TableHeap *table = table_info_->table_.get();
    table->UpdateTuple(New_tuple, *rid, txn);
    std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      Tuple key_tuple(
          Old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()));
      index_info->index_->DeleteEntry(key_tuple, *rid, txn);
      Tuple new_key_tuple(
          New_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()));
      index_info->index_->InsertEntry(new_key_tuple, *rid, txn);
      // 添加index_write_record
//      auto index_record = txn_->GetIndexWriteSet();
//      auto iter = index_record->begin();
//      while(iter != index_record->end()) {
//          if(iter->rid_ == *rid) {
//              assert(iter->tuple_.GetData() == Old_tuple.GetData());
//              iter->old_tuple_ = iter->tuple_;
//              iter->tuple_ = New_tuple;
//              iter->wtype_ = WType::UPDATE;
//          }
//      }
      IndexWriteRecord index_record(*rid, table_info_->oid_, WType::UPDATE, New_tuple, index_info->index_oid_, exec_ctx_->GetCatalog());
      index_record.old_tuple_ = Old_tuple;
      txn_->AppendTableWriteRecord(index_record);
    }
    // 释放锁
    if(lock_manager_ != nullptr && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        bool unlocked = lock_manager_->Unlock(txn_, *rid);
        if(!unlocked) {
            txn_->SetState((TransactionState::ABORTED));
            std::cout<<"unlock failed\n";
            return false;
        }
    }
    return true;
  }
  return false;
}
}  // namespace bustub
