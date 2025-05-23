/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "storage/db/db.h" // Include the header file for the Db class


RC UpdateStmt::create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt)
{
  if (db == nullptr) {
    return RC::INVALID_ARGUMENT;
  }
  const char *table_name = update_sql.relation_name.c_str();

  Table *table = nullptr;
  RC rc = db->find_table(table_name, table);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Table %s not found", table_name);
    return rc;
  }

  const TableMeta &table_meta = table->table_meta();
   //1.检查 表t1 有没有c1 列
   //2.检查 c1 列的类型 与 1 是否匹配
  const std::vector<FieldMeta>* fieldMeta = table_meta.field_metas();
  bool valid = false;
  FieldMeta update_field;
  for ( FieldMeta field : *fieldMeta) {
    if( 0 == strcmp(field.name(),update_sql.attribute_name.c_str()))
    {
      if(field.type() == update_sql.value.attr_type())
      {
        valid = true;
        update_field = field;
        break;
      }
    }
  }
  if(!valid)
  {
    LOG_WARN("Invalid field name or type");
    return RC::INVALID_ARGUMENT;
  }
  Value *values = new Value[1];
  values[0] = update_sql.value;
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));
  FilterStmt *filter_stmt = new FilterStmt();
  rc = FilterStmt::create(
    db, table, &table_map, update_sql.conditions.data(), static_cast<int>(update_sql.conditions.size()), filter_stmt);
if (rc != RC::SUCCESS) {
  LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
  return rc;
}

  stmt = new UpdateStmt(table, values, 1, update_field, filter_stmt);
  return RC::SUCCESS;
}
