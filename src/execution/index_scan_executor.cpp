//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    index_info_(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())),
    table_meta_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
    predicate_(plan->GetPredicate()) {}

void IndexScanExecutor::Init() {
    iter_ = static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>*>(index_info_->index_.get())->GetBeginIterator();
    end_iter_ = static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>*>(index_info_->index_.get())->GetEndIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
    while(iter_ != end_iter_) {
        [[maybe_unused]]auto Key_Value = *iter_;
        ++iter_;
        *rid = Key_Value.second;
        table_meta_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
        if(predicate_ == nullptr || predicate_->Evaluate(tuple, &table_meta_->schema_).GetAs<bool>()) {
            const Schema *output_schema = plan_->OutputSchema();
            std::vector<Value> values(output_schema->GetColumnCount());
            auto output_column = output_schema->GetColumns();
            for(size_t i = 0; i < values.size(); ++ i) {
                // values[i] = tuple->GetValue(&table_metadata_->schema_, table_metadata_->schema_.GetColIdx(output_column[i].GetName()));
                // values[i] = tuple->GetValue(&table_metadata_->schema_, static_cast<const ColumnValueExpression*>(output_column[i].GetExpr())->GetColIdx());
                values[i] = output_column[i].GetExpr()->Evaluate(tuple, &table_meta_->schema_);
            }
            *tuple = Tuple(values, output_schema);
            return true;
        }
    }
    return false;
}

}  // namespace bustub
