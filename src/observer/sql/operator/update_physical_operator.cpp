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
// Created by WangYunlai on 2022/6/27.
//

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];

  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    records_.emplace_back(std::move(record));
  }

  child->close();

  if (records_.empty()) {
    LOG_WARN("no records to update");
  }

  std::vector<char *> backup_datas;
  for (Record &record : records_) {
    Record backup_record;
    char *old_data = record.data();
    char *backup_data = (char *)malloc(record.len());
    memcpy(backup_data, old_data, record.len());

    backup_datas.push_back(backup_data);
  }

  std::pair<Value, FieldMeta> update_target = {value_, field_meta_};
  size_t update_num = 0;
  // 先收集记录再删除
  // 记录的有效性由事务来保证，如果事务不保证删除的有效性，那说明此事务类型不支持并发控制，比如VacuousTrx
  for (Record &record : records_) {

    rc = trx_->update_record(table_, record, update_target.second.name(), &update_target.first);
    ++update_num;
    if (rc != RC::SUCCESS) {
      // 如果更新失败，需要回滚，重新将修改过的元组复原
      LOG_WARN("failed to update record by transaction. rc=%s", strrc(rc));
      return rc;
    }
    // ++update_num;
    // if (rc != RC::SUCCESS) {
    //   // 如果更新失败，需要回滚，重新将修改过的元组复原
    //   for (int i = 0; i < (int)update_num; ++i) {
    //     char *backup_data = backup_datas[i];
    //     Record &record = records_[i];

    //     record.set_data(backup_data);
    //     table_->record_handler()->update_record(&record);
    //   }
    //   return rc;
    // }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  return RC::SUCCESS;
}
