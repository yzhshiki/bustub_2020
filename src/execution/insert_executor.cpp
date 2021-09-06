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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

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
    }
    return;
  }
  throw("tuple larger than one page size\n");
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!plan_->IsRawInsert()) {
    while (child_executor_->Next(tuple, rid)) {
      InsertTupleAndIndex(tuple, rid, exec_ctx_->GetTransaction());
    }
    return false;
  }
  const std::vector<std::vector<Value>> values = plan_->RawValues();
  for (const auto & value : values) {
    Tuple new_tuple(value, &table_metadata_->schema_);
    InsertTupleAndIndex(&new_tuple, rid, exec_ctx_->GetTransaction());
  }
  return false;
}

}  // namespace bustub
