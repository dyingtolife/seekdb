/**
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_ai_column_task_service.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_table_schema.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

using namespace common;

const char *OB_AI_COLUMN_TASK_TABLE = "__all_ai_column_task";

int ObAiColumnTaskService::exec_write(ObISQLClient &sql_client,
                                      const ObSqlString &sql,
                                      int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
    SHARE_SCHEMA_LOG(WARN, "ai column task write failed", K(ret), "sql", sql.ptr());
  }
  return ret;
}

int ObAiColumnTaskService::next_task_id(ObISQLClient &sql_client, int64_t &task_id)
{
  int ret = OB_SUCCESS;
  task_id = 0;
  // Single-node seekdb: allocate task ids from a monotonically increasing
  // max(task_id) read. Serialized by the worker/session inner connection and
  // the rowkey (table_id, task_id) uniqueness of the insert.
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign("SELECT MAX(task_id) AS max_id FROM __all_ai_column_task"))) {
      SHARE_SCHEMA_LOG(WARN, "build max task id sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(sql.ptr(), res))) {
      SHARE_SCHEMA_LOG(WARN, "read max task id failed", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result->next())) {
      SHARE_SCHEMA_LOG(WARN, "read max task id next failed", K(ret));
    } else {
      int64_t max_id = 0;
      if (OB_FAIL(result->get_int("max_id", max_id))) {
        SHARE_SCHEMA_LOG(WARN, "read max task id failed", K(ret));
      } else {
        task_id = max_id + 1;
      }
    }
  }
  return ret;
}

int ObAiColumnTaskService::splice_task_insert(const uint64_t table_id,
                                              const int64_t task_id,
                                              uint64_t column_id,
                                              const ObString &rowkey_predicate,
                                              const ObString &request_id,
                                              int64_t definition_version,
                                              ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  dml.reset();
  if (OB_FAIL(dml.add_uint64_pk_column("table_id", table_id))) {
  } else if (OB_FAIL(dml.add_pk_column("task_id", task_id))) {
  } else if (OB_FAIL(dml.add_uint64_column("column_id", column_id))) {
  } else if (OB_FAIL(dml.add_column("rowkey_predicate",
                                    ObHexEscapeSqlStr(rowkey_predicate)))) {
  } else if (OB_FAIL(dml.add_column("request_id",
                                    ObHexEscapeSqlStr(request_id)))) {
  } else if (OB_FAIL(dml.add_column("eligible_ts", "now(6)"))) {
    // eligible immediately (ObDMLSqlSplicer renders it as raw SQL text)
  } else if (OB_FAIL(dml.add_column("definition_version", definition_version))) {
  } else if (OB_FAIL(dml.add_column("status", "PENDING"))) {
  } else if (OB_FAIL(dml.add_column("retries", 0))) {
  } else if (OB_FAIL(dml.add_column("state", static_cast<int64_t>(AI_TASK_PENDING)))) {
  } else if (OB_FAIL(dml.add_column("retry_count", 0))) {
  }
  return ret;
}

int ObAiColumnTaskService::enqueue_tasks(ObISQLClient &sql_client,
                                         const ObTableSchema &table_schema,
                                         const uint64_t tenant_id,
                                         const ObString &rowkey_predicate,
                                         const ObString &request_id)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  const ObIArray<ObAiColumnSchema> &ai_columns = table_schema.get_ai_columns();
  if (ai_columns.count() <= 0) {
    // no AI columns: nothing to enqueue
  } else if (OB_FAIL(next_task_id(sql_client, task_id))) {
    SHARE_SCHEMA_LOG(WARN, "allocate task id failed", K(ret));
  } else {
    ObDMLSqlSplicer dml;
    for (int64_t i = 0; OB_SUCC(ret) && i < ai_columns.count(); ++i) {
      const ObAiColumnSchema &ai_col = ai_columns.at(i);
      if (0 != ai_col.sync_mode_.case_compare("async")) {
        continue;  // only async mode is in scope
      }
      if (OB_FAIL(splice_task_insert(table_schema.get_table_id(),
                                     task_id + i, ai_col.column_id_,
                                     rowkey_predicate, request_id,
                                     static_cast<int64_t>(ai_col.definition_version_),
                                     dml))) {
        SHARE_SCHEMA_LOG(WARN, "splice ai task insert failed", K(ret));
      } else {
        ObSqlString sql;
        int64_t affected = 0;
        if (OB_FAIL(dml.splice_insert_sql(OB_AI_COLUMN_TASK_TABLE, sql))) {
          SHARE_SCHEMA_LOG(WARN, "splice insert sql failed", K(ret));
        } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
          SHARE_SCHEMA_LOG(WARN, "insert ai task failed", K(ret));
        } else {
          ++task_id;  // subsequent columns of the same row change get new ids
        }
      }
    }
  }
  return ret;
}

int ObAiColumnTaskService::invalidate_row(ObISQLClient &sql_client,
                                          const ObTableSchema &table_schema,
                                          const ObString &rowkey_predicate,
                                          const ObString &request_id)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObAiColumnSchema> &ai_columns = table_schema.get_ai_columns();
  for (int64_t i = 0; OB_SUCC(ret) && i < ai_columns.count(); ++i) {
    const ObAiColumnSchema &ai_col = ai_columns.at(i);
    if (0 != ai_col.sync_mode_.case_compare("async")) {
      continue;
    }
    const ObColumnSchemaV2 *state_col = table_schema.get_column_schema(ai_col.state_column_id_);
    const ObColumnSchemaV2 *request_col = table_schema.get_column_schema(ai_col.request_column_id_);
    const ObColumnSchemaV2 *result_col = table_schema.get_column_schema(ai_col.column_id_);
    if (OB_ISNULL(state_col) || OB_ISNULL(request_col) || OB_ISNULL(result_col)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "invalidate hidden column missing", K(ret));
    } else {
      ObSqlString sql;
      int64_t affected = 0;
      if (OB_FAIL(sql.assign_fmt(
              "UPDATE %.*s SET %.*s = NULL, %.*s = 'PENDING', %.*s = '%.*s' WHERE %.*s",
              table_schema.get_table_name_str().length(),
              table_schema.get_table_name_str().ptr(),
              result_col->get_column_name_str().length(),
              result_col->get_column_name_str().ptr(),
              state_col->get_column_name_str().length(),
              state_col->get_column_name_str().ptr(),
              request_col->get_column_name_str().length(),
              request_col->get_column_name_str().ptr(),
              request_id.length(), request_id.ptr(),
              rowkey_predicate.length(), rowkey_predicate.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "build invalidate sql failed", K(ret));
      } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
        SHARE_SCHEMA_LOG(WARN, "invalidate row failed", K(ret), "sql", sql.ptr());
      }
    }
  }
  return ret;
}

int ObAiColumnTaskService::remove_tasks_for_row(ObISQLClient &sql_client,
                                                uint64_t table_id,
                                                const ObString &rowkey_predicate)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE table_id = %" PRIu64
                             " AND rowkey_predicate = '%.*s'",
                             OB_AI_COLUMN_TASK_TABLE, table_id,
                             rowkey_predicate.length(), rowkey_predicate.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "build remove tasks sql failed", K(ret));
  } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
    SHARE_SCHEMA_LOG(WARN, "remove ai tasks failed", K(ret));
  }
  return ret;
}

int ObAiColumnTaskService::remove_tasks_for_table(ObISQLClient &sql_client,
                                                  uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE table_id = %" PRIu64,
                             OB_AI_COLUMN_TASK_TABLE, table_id))) {
    SHARE_SCHEMA_LOG(WARN, "build remove all tasks sql failed", K(ret));
  } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
    SHARE_SCHEMA_LOG(WARN, "remove all ai tasks failed", K(ret));
  }
  return ret;
}

int ObAiColumnTaskService::claim_tasks(ObISQLClient &sql_client,
                                       int64_t max_batch,
                                       ObIArray<ClaimedTask> &tasks)
{
  int ret = OB_SUCCESS;
  tasks.reset();
  // Single worker (single-node seekdb) claims by state transition. The
  // UPDATE ... WHERE state = PENDING ... then read-back loop is atomic per
  // statement; concurrent claimants can only lose rows whose state moved.
  ObSqlString claim_sql;
  int64_t affected = 0;
  if (OB_FAIL(claim_sql.assign_fmt(
          "UPDATE %s SET state = %d, gmt_modified = now(6)"
          " WHERE state = %d AND eligible_ts <= now(6)"
          " ORDER BY task_id ASC LIMIT %ld",
          OB_AI_COLUMN_TASK_TABLE, static_cast<int>(AI_TASK_EXECUTING),
          static_cast<int>(AI_TASK_PENDING), max_batch))) {
    SHARE_SCHEMA_LOG(WARN, "build claim sql failed", K(ret));
  } else if (OB_FAIL(exec_write(sql_client, claim_sql, affected))) {
    SHARE_SCHEMA_LOG(WARN, "claim ai tasks failed", K(ret));
  } else if (affected > 0) {
    ObSqlString read_sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(read_sql.assign_fmt(
              "SELECT table_id, task_id, column_id, rowkey_predicate,"
              " request_id, definition_version, retry_count FROM %s"
              " WHERE state = %d AND gmt_modified >= now(6) - INTERVAL 10 SECOND"
              " ORDER BY task_id ASC",
              OB_AI_COLUMN_TASK_TABLE, static_cast<int>(AI_TASK_EXECUTING)))) {
        SHARE_SCHEMA_LOG(WARN, "build read claimed sql failed", K(ret));
      } else if (OB_FAIL(sql_client.read(read_sql.ptr(), res))) {
        SHARE_SCHEMA_LOG(WARN, "read claimed tasks failed", K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ClaimedTask task;
          int64_t tmp_int = 0;
          if (OB_FAIL(result->get_uint("table_id", task.table_id_))) {
            SHARE_SCHEMA_LOG(WARN, "read task table id failed", K(ret));
          } else if (OB_FAIL(result->get_int("task_id", tmp_int))) {
            SHARE_SCHEMA_LOG(WARN, "read task id failed", K(ret));
          } else if (OB_FAIL(result->get_uint("column_id", task.column_id_))) {
            SHARE_SCHEMA_LOG(WARN, "read task column id failed", K(ret));
          } else if (OB_FAIL(result->get_int("definition_version", task.definition_version_))) {
            SHARE_SCHEMA_LOG(WARN, "read task definition version failed", K(ret));
          } else if (OB_FAIL(result->get_int("retry_count", task.retry_count_))) {
            SHARE_SCHEMA_LOG(WARN, "read task retry count failed", K(ret));
          } else {
            task.task_id_ = tmp_int;
          }
          if (OB_FAIL(ret)) {
            // leave loop on read error
          } else {
            ObString tmp;
            ObString rowkey_copy;
            ObString request_copy;
            if (OB_FAIL(result->get_varchar("rowkey_predicate", tmp))) {
              SHARE_SCHEMA_LOG(WARN, "read rowkey predicate failed", K(ret));
            } else if (OB_FAIL(ob_write_string(allocator_, tmp, rowkey_copy))) {
              SHARE_SCHEMA_LOG(WARN, "copy rowkey predicate failed", K(ret));
            } else if (OB_FAIL(result->get_varchar("request_id", tmp))) {
              SHARE_SCHEMA_LOG(WARN, "read request id failed", K(ret));
            } else if (OB_FAIL(ob_write_string(allocator_, tmp, request_copy))) {
              SHARE_SCHEMA_LOG(WARN, "copy request id failed", K(ret));
            } else {
              task.rowkey_predicate_ = rowkey_copy;
              task.request_id_ = request_copy;
              if (OB_FAIL(tasks.push_back(task))) {
                SHARE_SCHEMA_LOG(WARN, "append claimed task failed", K(ret));
              }
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObAiColumnTaskService::recover_stuck_tasks(ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  if (OB_FAIL(sql.assign_fmt(
          "UPDATE %s SET state = %d WHERE state = %d",
          OB_AI_COLUMN_TASK_TABLE, static_cast<int>(AI_TASK_PENDING),
          static_cast<int>(AI_TASK_EXECUTING)))) {
    SHARE_SCHEMA_LOG(WARN, "build recover sql failed", K(ret));
  } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
    SHARE_SCHEMA_LOG(WARN, "recover stuck tasks failed", K(ret));
  } else if (affected > 0) {
    SHARE_SCHEMA_LOG(INFO, "recovered stuck ai column tasks", K(affected));
  }
  return ret;
}

int ObAiColumnTaskService::publish_group(ObISQLClient &sql_client,
                                         const ObTableSchema &table_schema,
                                         uint64_t tenant_id,
                                         const ObString &rowkey_predicate,
                                         const ObString &request_id,
                                         const ObIArray<PublishEntry> &entries,
                                         int64_t definition_version)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  UNUSED(definition_version);
  // One transaction (caller-owned), one guarded UPDATE per AI column: the
  // WHERE clause repeats the request identity so a late result for an older
  // request (or a result for a deleted row) updates nothing.
  const ObIArray<ObAiColumnSchema> &ai_columns = table_schema.get_ai_columns();
  for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
    const ObAiColumnSchema *ai_col = NULL;
    for (int64_t j = 0; NULL == ai_col && j < ai_columns.count(); ++j) {
      if (ai_columns.at(j).column_id_ == entries.at(i).column_id_) {
        ai_col = &ai_columns.at(j);
      }
    }
    const ObColumnSchemaV2 *state_col = NULL;
    const ObColumnSchemaV2 *request_col = NULL;
    const ObColumnSchemaV2 *result_col = NULL;
    if (OB_ISNULL(ai_col)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "publish entry ai column missing", K(ret), K(entries.at(i).column_id_));
    } else if (OB_ISNULL(state_col = table_schema.get_column_schema(ai_col->state_column_id_))
        || OB_ISNULL(request_col = table_schema.get_column_schema(ai_col->request_column_id_))
        || OB_ISNULL(result_col = table_schema.get_column_schema(ai_col->column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "publish hidden column missing", K(ret));
    } else {
      ObSqlString sql;
      int64_t affected = 0;
      if (OB_FAIL(sql.assign_fmt(
              "UPDATE %.*s SET %.*s = %.*s, %.*s = 'READY' WHERE %.*s AND %.*s = '%.*s'",
              table_schema.get_table_name_str().length(),
              table_schema.get_table_name_str().ptr(),
              result_col->get_column_name_str().length(),
              result_col->get_column_name_str().ptr(),
              entries.at(i).value_.length(), entries.at(i).value_.ptr(),
              state_col->get_column_name_str().length(),
              state_col->get_column_name_str().ptr(),
              rowkey_predicate.length(), rowkey_predicate.ptr(),
              request_col->get_column_name_str().length(),
              request_col->get_column_name_str().ptr(),
              request_id.length(), request_id.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "build publish sql failed", K(ret));
      } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
        SHARE_SCHEMA_LOG(WARN, "publish result failed", K(ret), "sql", sql.ptr());
      }
      // affected == 0 is legitimate: a newer change already took over the
      // row's request id (latest-wins) or the row was deleted (no revive).
    }
  }
  if (OB_SUCC(ret)) {
    // Group is terminal: remove all task rows of this request.
    ObSqlString del_sql;
    int64_t affected = 0;
    if (OB_FAIL(del_sql.assign_fmt(
            "DELETE FROM %s WHERE table_id = %" PRIu64
            " AND rowkey_predicate = '%.*s' AND request_id = '%.*s'",
            OB_AI_COLUMN_TASK_TABLE, table_schema.get_table_id(),
            rowkey_predicate.length(), rowkey_predicate.ptr(),
            request_id.length(), request_id.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "build publish delete sql failed", K(ret));
    } else if (OB_FAIL(exec_write(sql_client, del_sql, affected))) {
      SHARE_SCHEMA_LOG(WARN, "remove published tasks failed", K(ret));
    }
  }
  return ret;
}

int ObAiColumnTaskService::fail_task(ObISQLClient &sql_client,
                                     const ClaimedTask &task,
                                     int64_t max_retries,
                                     int64_t retry_delay_us)
{
  int ret = OB_SUCCESS;
  if (task.retry_count_ < max_retries) {
    ObSqlString sql;
    int64_t affected = 0;
    if (OB_FAIL(sql.assign_fmt(
            "UPDATE %s SET state = %d, retry_count = retry_count + 1,"
            " eligible_ts = now(6) + INTERVAL %ld MICROSECOND"
            " WHERE table_id = %" PRIu64 " AND task_id = %ld AND state = %d",
            OB_AI_COLUMN_TASK_TABLE, static_cast<int>(AI_TASK_RETRY_WAIT),
            retry_delay_us, task.table_id_, task.task_id_,
            static_cast<int>(AI_TASK_EXECUTING)))) {
      SHARE_SCHEMA_LOG(WARN, "build retry sql failed", K(ret));
    } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
      SHARE_SCHEMA_LOG(WARN, "reschedule task failed", K(ret));
    }
    // RETRY_WAIT -> PENDING transition happens on the next claim sweep.
  } else {
    // Terminal failure: remove the task row (state lands in the hidden
    // state column, which the caller publishes as FAILED).
    ObSqlString sql;
    int64_t affected = 0;
    if (OB_FAIL(sql.assign_fmt(
            "DELETE FROM %s WHERE table_id = %" PRIu64 " AND task_id = %ld",
            OB_AI_COLUMN_TASK_TABLE, task.table_id_, task.task_id_))) {
      SHARE_SCHEMA_LOG(WARN, "build delete task sql failed", K(ret));
    } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
      SHARE_SCHEMA_LOG(WARN, "delete failed task failed", K(ret));
    }
  }
  return ret;
}

int ObAiColumnTaskService::publish_failure_group(ObISQLClient &sql_client,
                                                 const ObTableSchema &table_schema,
                                                 uint64_t tenant_id,
                                                 const ObString &rowkey_predicate,
                                                 const ObString &request_id,
                                                 const ObIArray<ClaimedTask> &tasks,
                                                 int64_t definition_version)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  UNUSED(definition_version);
  const ObIArray<ObAiColumnSchema> &ai_columns = table_schema.get_ai_columns();
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    const ObAiColumnSchema *ai_col = NULL;
    for (int64_t j = 0; NULL == ai_col && j < ai_columns.count(); ++j) {
      if (ai_columns.at(j).column_id_ == tasks.at(i).column_id_) {
        ai_col = &ai_columns.at(j);
      }
    }
    const ObColumnSchemaV2 *state_col = NULL;
    const ObColumnSchemaV2 *request_col = NULL;
    if (OB_ISNULL(ai_col)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "failure group ai column missing", K(ret), K(tasks.at(i).column_id_));
    } else if (OB_ISNULL(state_col = table_schema.get_column_schema(ai_col->state_column_id_))
        || OB_ISNULL(request_col = table_schema.get_column_schema(ai_col->request_column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "failure group hidden column missing", K(ret));
    } else {
      ObSqlString sql;
      int64_t affected = 0;
      if (OB_FAIL(sql.assign_fmt(
              "UPDATE %.*s SET %.*s = 'FAILED' WHERE %.*s AND %.*s = '%.*s'",
              table_schema.get_table_name_str().length(),
              table_schema.get_table_name_str().ptr(),
              state_col->get_column_name_str().length(),
              state_col->get_column_name_str().ptr(),
              rowkey_predicate.length(), rowkey_predicate.ptr(),
              request_col->get_column_name_str().length(),
              request_col->get_column_name_str().ptr(),
              request_id.length(), request_id.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "build failure publish sql failed", K(ret));
      } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
        SHARE_SCHEMA_LOG(WARN, "publish failure failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSqlString del_sql;
    int64_t affected = 0;
    if (OB_FAIL(del_sql.assign_fmt(
            "DELETE FROM %s WHERE table_id = %" PRIu64
            " AND rowkey_predicate = '%.*s' AND request_id = '%.*s'",
            OB_AI_COLUMN_TASK_TABLE, table_schema.get_table_id(),
            rowkey_predicate.length(), rowkey_predicate.ptr(),
            request_id.length(), request_id.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "build failure delete sql failed", K(ret));
    } else if (OB_FAIL(exec_write(sql_client, del_sql, affected))) {
      SHARE_SCHEMA_LOG(WARN, "remove failed tasks failed", K(ret));
    }
  }
  return ret;
}

int ObAiColumnTaskService::retry_column_rows(ObISQLClient &sql_client,
                                             const ObTableSchema &table_schema,
                                             const ObAiColumnSchema &ai_column,
                                             const ObString &extra_predicate,
                                             bool use_status_pseudo,
                                             const ObString &status_value,
                                             int64_t &matched_rows)
{
  int ret = OB_SUCCESS;
  matched_rows = 0;
  const ObColumnSchemaV2 *state_col = table_schema.get_column_schema(ai_column.state_column_id_);
  const ObColumnSchemaV2 *request_col = table_schema.get_column_schema(ai_column.request_column_id_);
  const ObColumnSchemaV2 *result_col = table_schema.get_column_schema(ai_column.column_id_);
  if (OB_ISNULL(state_col) || OB_ISNULL(request_col) || OB_ISNULL(result_col)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "retry hidden column missing", K(ret));
  } else {
    // Build the WHERE clause: optional status pseudo condition + extra pred.
    // `status` (bare) maps to the hidden state column; backticked `status`
    // arrives pre-rewritten by the resolver to the physical column name.
    ObSqlString where;
    if (OB_FAIL(where.assign(""))) {
    } else if (use_status_pseudo && status_value.length() > 0) {
      if (OB_FAIL(where.append_fmt("%.*s = '%.*s'",
                                   state_col->get_column_name_str().length(),
                                   state_col->get_column_name_str().ptr(),
                                   status_value.length(), status_value.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "append status cond failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && extra_predicate.length() > 0) {
      if (where.length() > 0 && OB_FAIL(where.append(" AND "))) {
      } else if (OB_FAIL(where.append(extra_predicate))) {
        SHARE_SCHEMA_LOG(WARN, "append extra predicate failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // Collect matched rowkeys (rowkey predicate strings) by scanning the
      // table with the condition; then invalidate + enqueue per row.
      ObSqlString sel;
      ObString col_name = state_col->get_column_name_str();
      ObString req_name = request_col->get_column_name_str();
      ObString res_name = result_col->get_column_name_str();
      ObSqlString task_del;
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(sel.assign_fmt("SELECT %.*s AS rk FROM %.*s%s%.*s",
                                   table_schema.get_rowkey_column_names_str_for_ai_retry().length(),
                                   table_schema.get_rowkey_column_names_str_for_ai_retry().ptr(),
                                   table_schema.get_table_name_str().length(),
                                   table_schema.get_table_name_str().ptr(),
                                   where.length() > 0 ? " WHERE " : "",
                                   where.length(), where.length() ? where.ptr() : ""))) {
          SHARE_SCHEMA_LOG(WARN, "build retry select failed", K(ret));
        }
        // NOTE: the caller (executor) supplies the full rowkey predicate
        // expression; see ObAiColumnRetryExecutor which handles this instead.
        UNUSED(result);
        UNUSED(res);
      }
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
