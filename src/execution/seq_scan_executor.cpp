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
      predicate_(plan->GetPredicate()) {}

void SeqScanExecutor::Init() {
  iter_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != iter_end_) {
    *rid = iter_->GetRid();
    *tuple = *iter_;
    iter_++;
    if (predicate_ == nullptr || predicate_->Evaluate(tuple, &table_metadata_->schema_).GetAs<bool>()) {
      const Schema *output_schema = plan_->OutputSchema();
      std::vector<Value> values(output_schema->GetColumnCount());
      auto output_column = output_schema->GetColumns();
      for (size_t i = 0; i < values.size(); ++i) {
        // values[i] = tuple->GetValue(&table_metadata_->schema_,
        // table_metadata_->schema_.GetColIdx(output_column[i].GetName())); values[i] =
        // tuple->GetValue(&table_metadata_->schema_, static_cast<const
        // ColumnValueExpression*>(output_column[i].GetExpr())->GetColIdx());
        values[i] = output_column[i].GetExpr()->Evaluate(tuple, &table_metadata_->schema_);
      }
      *tuple = Tuple(values, output_schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
