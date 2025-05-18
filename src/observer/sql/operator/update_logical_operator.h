#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"

/**
 * @brief 逻辑算子，用于执行update语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  /**
   * @brief 构造函数
   * @param table 要更新的表
   * @param field_name 要更新的字段名
   * @param value 更新的值
   */
  UpdateLogicalOperator(Table *table, const FieldMeta &field_meta, const Value &value);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  OpType              get_op_type() const override { return OpType::LOGICALUPDATE; }

  Table              *table() const { return table_; }
  const FieldMeta  &field_meta() const { return field_meta_; }
  const Value        &value() const { return value_; }

private:
  Table       *table_ = nullptr;       // 要更新的表
  FieldMeta  field_meta_;            // 要更新的字段名
  Value        value_;                 // 更新的值
};