//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      outer_ended_(false),
      inner_table(exec_ctx->GetCatalog()->GetTable(plan->GetInnerTableOid())),
      inner_index(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexName(), inner_table->name_)),
      predicate_(plan_->Predicate()) {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  child_executor_->Next(&last_outer_tuple_, &last_outer_rid_);
  outer_ended_ = false;
  // 从谓词拿到outer那一边的子expr，包含对哪一列join的信息，子expr形如auto colA =
  // MakeColumnValueExpression(*outer_schema1, 0, "colA");
  const ColumnValueExpression *outer_expr = dynamic_cast<const ColumnValueExpression *>(predicate_->GetChildAt(0));
  // 子expr中存有其列在outer table outputschema的列号
  auto colid_in_outSchema = outer_expr->GetColIdx();
  auto col_name = plan_->OutputSchema()->GetColumn(colid_in_outSchema).GetName();
  // 得到此列在子节点outputschema的列号，然后取得此列并用之构造key_schema
  colid_in_childschema_ = child_executor_->GetOutputSchema()->GetColIdx(col_name);
}

/*
bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
    Tuple *l_tuple = &last_outer_tuple_;
    Tuple r_tuple;
    // 想法：在outer表的某一tuple在寻找inner tuples时，Next可能被多次调用
    // 于是使用一个vector存储outer tuple对应的所有inner tuples，每次Next返回一条。
    if (!last_joined_tuples.empty()) {
      *tuple = *last_joined_tuples.back();
      delete last_joined_tuples.back();
      last_joined_tuples.pop_back();
      return true;
    }
    // 如果上一条outer tuple已join结束，就将下一个outer tuple
    // 该join的所有inner tuple 放进last_joined_tuples，并自我调用
    while (!outer_ended_) {
      // 从谓词拿到outer那一边的子expr，包含对哪一列join的信息，子expr形如auto colA =
      // MakeColumnValueExpression(*outer_schema1, 0, "colA");
      const ColumnValueExpression *outer_expr = dynamic_cast<const ColumnValueExpression *>(predicate_->GetChildAt(0));
      // 子expr中存有其列在outer table outputschema的列号
      auto colid_in_outSchema = outer_expr->GetColIdx();
      auto col_name = plan_->OutputSchema()->GetColumn(colid_in_outSchema).GetName();
      // 得到此列在子节点outputschema的列号，然后取得此列并用之构造key_schema
      auto colid_in_childschema = child_executor_->GetOutputSchema()->GetColIdx(col_name);
      std::vector<uint32_t> key_attrs;
      key_attrs.push_back(colid_in_childschema);
      std::vector<Column> key_columns;
      key_columns.push_back(child_executor_->GetOutputSchema()->GetColumn(colid_in_childschema));
      Schema key_schema(key_columns);
      // 构建key_tuple，用之在inner index上搜索拥有key值的inner tuple的rid
      Tuple key_tuple = last_outer_tuple_.KeyFromTuple(*child_executor_->GetOutputSchema(), key_schema, key_attrs);
      std::vector<RID> result_rid;
      inner_index->index_->ScanKey(key_tuple, &result_rid, exec_ctx_->GetTransaction());
      for (RID inner_rid : result_rid) {
        inner_table->table_->GetTuple(inner_rid, &r_tuple, exec_ctx_->GetTransaction());
        std::vector<Value> final_values;
        for (uint32_t col_id = 0; col_id < plan_->OuterTableSchema()->GetColumnCount(); col_id++) {
          final_values.push_back(l_tuple->GetValue(plan_->OuterTableSchema(), col_id));
        }
        for (uint32_t col_id = 0; col_id < plan_->InnerTableSchema()->GetColumnCount(); col_id++) {
          final_values.push_back(r_tuple.GetValue(plan_->InnerTableSchema(), col_id));
        }
        Tuple *final_tuple = new Tuple(final_values, plan_->OutputSchema());
        last_joined_tuples.push_back(final_tuple);
      }
      outer_ended_ = !child_executor_->Next(&last_outer_tuple_, &last_outer_rid_);
      if (!last_joined_tuples.empty()) {
        *tuple = *last_joined_tuples.back();
        delete last_joined_tuples.back();
        last_joined_tuples.pop_back();
        return true;
      }
      return Next(tuple, rid);
    }
  return false;
}
*/

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  [[maybe_unused]] Tuple *l_tuple = &last_outer_tuple_;
  [[maybe_unused]] Tuple r_tuple;
  // 如果上一条outer tuple已join结束，就将下一个outer tuple
  // 该join的所有inner tuple 放进last_joined_tuples，并自我调用
  while (!outer_ended_) {
    //   // 从谓词拿到outer那一边的子expr，包含对哪一列join的信息，子expr形如auto colA =
    //   // MakeColumnValueExpression(*outer_schema1, 0, "colA");
    //   const ColumnValueExpression *outer_expr = dynamic_cast<const ColumnValueExpression
    //   *>(predicate_->GetChildAt(0));
    //   // 子expr中存有其列在outer table outputschema的列号
    //   auto colid_in_outSchema = outer_expr->GetColIdx();
    //   auto col_name = plan_->OutputSchema()->GetColumn(colid_in_outSchema).GetName();
    //   // 得到此列在子节点outputschema的列号，然后取得此列并用之构造key_schema
    //   auto colid_in_childschema = child_executor_->GetOutputSchema()->GetColIdx(col_name);
    //   std::vector<uint32_t> key_attrs;
    //   key_attrs.push_back(colid_in_childschema);
    //   std::vector<Column> key_columns;
    //   key_columns.push_back(child_executor_->GetOutputSchema()->GetColumn(colid_in_childschema));
    //   Schema key_schema(key_columns);
    //   // 构建key_tuple，用之在inner index上搜索拥有key值的inner tuple的rid
    //   Tuple key_tuple = last_outer_tuple_.KeyFromTuple(*child_executor_->GetOutputSchema(), key_schema, key_attrs);
    // std::vector<Value> key_value;
    // key_value.push_back(last_outer_tuple_.GetValue(plan_->OuterTableSchema(), colid_in_childschema_));
    // const Tuple key_tuple(key_value, &inner_index->key_schema_);
    Tuple key_tuple = last_outer_tuple_.KeyFromTuple(*child_executor_->GetOutputSchema(), inner_index->key_schema_,
                                                     {colid_in_childschema_});
    [[maybe_unused]] std::vector<RID> result_rid;
    inner_index->index_->ScanKey(key_tuple, &result_rid, exec_ctx_->GetTransaction());
    // RID tmp;
    // result_rid.push_back(tmp);
    if (!result_rid.empty()) {
      RID inner_rid = result_rid[0];
      inner_table->table_->GetTuple(inner_rid, &r_tuple, exec_ctx_->GetTransaction());
      std::vector<Value> final_values;
      for (uint32_t col_id = 0; col_id < plan_->OuterTableSchema()->GetColumnCount(); col_id++) {
        final_values.push_back(l_tuple->GetValue(plan_->OuterTableSchema(), col_id));
      }
      for (uint32_t col_id = 0; col_id < plan_->InnerTableSchema()->GetColumnCount(); col_id++) {
        final_values.push_back(r_tuple.GetValue(plan_->InnerTableSchema(), col_id));
      }
      Tuple final_tuple(final_values, plan_->OutputSchema());
      *tuple = final_tuple;
      *rid = tuple->GetRid();
    }
    outer_ended_ = !child_executor_->Next(&last_outer_tuple_, &last_outer_rid_);
    if (!result_rid.empty()) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
