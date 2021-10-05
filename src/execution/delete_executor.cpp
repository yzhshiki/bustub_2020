//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
      txn_(exec_ctx_->GetTransaction()),
      lock_manager_(exec_ctx_->GetLockManager()) {}

void DeleteExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
      if(lock_manager_ != nullptr && txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
          if(!txn_->IsSharedLocked(*rid) && !txn_->IsExclusiveLocked(*rid)) {
              try {
                  bool locked = lock_manager_->LockExclusive(txn_, *rid);
                  if(!locked) {
                      txn_->SetState(TransactionState::ABORTED);
                      return false;
                  }
              }
              catch (TransactionAbortException &e) {
                std::cout << e.GetInfo();
              }
          }
      }
    try {
      TableMetadata *table_matadata = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
      TableHeap *table = table_matadata->table_.get();
      table->MarkDelete(*rid, exec_ctx_->GetTransaction());
      std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_matadata->name_);
      for (auto indexinfo : indexes) {
        Tuple index_key(
            tuple->KeyFromTuple(table_matadata->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()));
        indexinfo->index_->DeleteEntry(index_key, *rid, exec_ctx_->GetTransaction());
      IndexWriteRecord index_record(*rid, table_matadata->oid_, WType::DELETE, *tuple, indexinfo->index_oid_, exec_ctx_->GetCatalog());
      txn_->AppendTableWriteRecord(index_record);
      }
    } catch (const std::exception &e) {
      std::cerr << "Delete error\n";
    }
      if(lock_manager_ != nullptr && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
          bool unlocked = lock_manager_->Unlock(txn_, *rid);
          if(!unlocked) {
              txn_->SetState(TransactionState::ABORTED);
              return false;
          }
      }
  }
  return false;
}

}  // namespace bustub
