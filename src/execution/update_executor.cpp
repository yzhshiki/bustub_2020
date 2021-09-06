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
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple Old_tuple;
  Tuple New_tuple;
  Transaction *txn = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&Old_tuple, rid)) {
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
    }
    return true;
  }
  return false;
}
}  // namespace bustub
