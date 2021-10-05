//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),txn_(exec_ctx_->GetTransaction()),
      lock_manager_(exec_ctx_->GetLockManager()) {}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata_->name_);
}

void InsertExecutor::InsertTupleAndIndex(Tuple *tuple, RID *rid, Transaction *txn) {
  if (table_metadata_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    for (auto index : table_indexes) {
      Tuple index_key(tuple->KeyFromTuple(table_metadata_->schema_, index->key_schema_, index->index_->GetKeyAttrs()));
      index->index_->InsertEntry(index_key, *rid, txn);
//      TableWriteRecord table_record(*rid, WType::INSERT, *tuple, table_metadata_->table_.get());
//      txn->AppendTableWriteRecord(table_record);
        // 更新index_write_record
//      IndexWriteRecord index_record(*rid, plan_->TableOid(), WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog());
//      txn->AppendTableWriteRecord(index_record);
    }
    return;
  }
  throw("tuple larger than one page size\n");
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
//    if(lock_manager_ != nullptr && txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
//        if(!txn_->IsExclusiveLocked(*rid) && !txn_->IsSharedLocked(*rid)) {
//            try {
//                bool locked = lock_manager_->LockExclusive(txn_, *rid);
//                if(!locked) {
//                    txn_->SetState(TransactionState::ABORTED);
//                    return false;
//                }
//            }
//            catch (TransactionAbortException &e) {
//                std::cout<<e.GetInfo();
//            }
//        }
//    }
  if (!plan_->IsRawInsert()) {
    while (child_executor_->Next(tuple, rid)) {
      InsertTupleAndIndex(tuple, rid, exec_ctx_->GetTransaction());
    }
    // 可能要解锁
//    if(lock_manager_ != nullptr && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
//        bool unlocked = lock_manager_->Unlock(txn_, *rid);
//        if(!unlocked) {
//            txn_->SetState((TransactionState::ABORTED));
//            std::cout<<"unlock failed\n";
//            return false;
//        }
//    }
    return false;
  }
  const std::vector<std::vector<Value>> values = plan_->RawValues();
  for (const auto &value : values) {
    Tuple new_tuple(value, &table_metadata_->schema_);
    InsertTupleAndIndex(&new_tuple, rid, exec_ctx_->GetTransaction());
  }
  // 可能要解锁
//    if(lock_manager_ != nullptr && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
//        bool unlocked = lock_manager_->Unlock(txn_, *rid);
//        if(!unlocked) {
//            txn_->SetState((TransactionState::ABORTED));
//            std::cout<<"unlock failed\n";
//            return false;
//        }
//    }
  return false;
}

}  // namespace bustub
