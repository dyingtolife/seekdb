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
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/object/ob_object.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

using namespace common;
using namespace common::sqlclient;

namespace
{
// Renders "`db`.`table`" for the data table of an AI column task. Statements
// built here may run over a connection whose default database is not the
// table's database (session inner connection: the user database; worker: the
// server SQL proxy), so the data table must be schema-qualified as well.
int get_qualified_table_name(const ObTableSchema &table_schema, ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObDatabaseSchema *db_schema = NULL;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_runtime_schema_guard(guard))) {
    SHARE_SCHEMA_LOG(ERROR, "get runtime schema guard failed", K(ret));
  } else if (OB_FAIL(guard.get_database_schema(table_schema.get_database_id(), db_schema))) {
    SHARE_SCHEMA_LOG(ERROR, "get database schema failed", K(ret),
                     K(table_schema.get_database_id()));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(ERROR, "database schema missing", K(ret),
                     K(table_schema.get_database_id()));
  } else if (OB_FAIL(out.assign_fmt("`%.*s`.`%.*s`",
                                    db_schema->get_database_name_str().length(),
                                    db_schema->get_database_name_str().ptr(),
                                    table_schema.get_table_name_str().length(),
                                    table_schema.get_table_name_str().ptr()))) {
    SHARE_SCHEMA_LOG(ERROR, "build qualified table name failed", K(ret));
  }
  return ret;
}
}  // namespace

// The queue lives in the system database. The AI column DML linkage runs its
// statements over the *session inner connection*, whose default database is
// the user database, and the background worker runs over the server SQL proxy:
// an unqualified table name resolves against the current database and fails
// with 1146 (table does not exist). Statements are schema-qualified below.
const char *OB_AI_COLUMN_TASK_TABLE = "oceanbase.__all_ai_column_task";

int ObAiColumnTaskService::exec_write(ObISQLClient &sql_client,
                                      const ObSqlString &sql,
                                      int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
    // ERROR level on purpose: this build compiles out WARN, and a silent write
    // failure here drops the whole AI linkage without any trace.
    SHARE_SCHEMA_LOG(ERROR, "ai column task write failed", K(ret), "sql", sql.ptr());
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
    // IFNULL + CAST: MAX() over an empty queue is NULL and the inner SQL
    // result rejects NULL/non-ObIntTC values (OB_ERR_NULL_VALUE /
    // OB_OBJ_TYPE_ERROR), which used to drop the whole enqueue silently.
    if (OB_FAIL(sql.assign_fmt(
            "SELECT CAST(IFNULL(MAX(task_id), 0) AS SIGNED) AS max_id FROM %s",
            OB_AI_COLUMN_TASK_TABLE))) {
      SHARE_SCHEMA_LOG(ERROR, "build max task id sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
      SHARE_SCHEMA_LOG(ERROR, "read max task id failed", K(ret), "sql", sql.ptr());
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(ERROR, "max task id result missing", K(ret), "sql", sql.ptr());
    } else if (OB_FAIL(result->next())) {
      SHARE_SCHEMA_LOG(ERROR, "read max task id next failed", K(ret), "sql", sql.ptr());
    } else {
      int64_t max_id = 0;
      if (OB_FAIL(result->get_int("max_id", max_id))) {
        SHARE_SCHEMA_LOG(ERROR, "read max task id value failed", K(ret), "sql", sql.ptr());
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
  } else if (OB_FAIL(dml.add_column("eligible_ts",
                                    // ObDMLSqlSplicer quotes every non-PK value, so a raw
                                    // now(6) cannot be expressed here: the literal string
                                    // 'now(6)' reached a timestamp column and failed with
                                    // OB_INVALID_DATE_VALUE (-4219). A fixed timestamp in
                                    // the past means "eligible immediately" whatever the
                                    // session time zone is.
                                    "1970-01-02 00:00:00.000000"))) {
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
    SHARE_SCHEMA_LOG(ERROR, "allocate task id failed", K(ret),
                     K(table_schema.get_table_id()));
  } else {
    SHARE_SCHEMA_LOG(ERROR, "ai enqueue begin", K(task_id),
                     K(table_schema.get_table_id()), "ai_cols", ai_columns.count(),
                     "rowkey", rowkey_predicate, K(request_id));
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
          SHARE_SCHEMA_LOG(ERROR, "splice insert sql failed", K(ret));
        } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
          SHARE_SCHEMA_LOG(ERROR, "insert ai task failed", K(ret), K(task_id),
                           K(ai_col.column_id_), "sql", sql.ptr());
        } else {
          SHARE_SCHEMA_LOG(ERROR, "ai task enqueued", K(task_id), K(ai_col.column_id_),
                           K(affected), "sql", sql.ptr());
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
      ObSqlString qualified_table;
      ObSqlString sql;
      int64_t affected = 0;
      if (OB_FAIL(get_qualified_table_name(table_schema, qualified_table))) {
        SHARE_SCHEMA_LOG(ERROR, "build qualified table name failed", K(ret),
                         K(table_schema.get_database_id()));
      } else if (OB_FAIL(sql.assign_fmt(
              "UPDATE %.*s SET `%.*s` = NULL, `%.*s` = 'PENDING', `%.*s` = '%.*s'"
              " WHERE %.*s",
              qualified_table.length(), qualified_table.ptr(),
              result_col->get_column_name_str().length(),
              result_col->get_column_name_str().ptr(),
              state_col->get_column_name_str().length(),
              state_col->get_column_name_str().ptr(),
              request_col->get_column_name_str().length(),
              request_col->get_column_name_str().ptr(),
              request_id.length(), request_id.ptr(),
              rowkey_predicate.length(), rowkey_predicate.ptr()))) {
        SHARE_SCHEMA_LOG(ERROR, "build invalidate sql failed", K(ret));
      } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
        SHARE_SCHEMA_LOG(ERROR, "invalidate row failed", K(ret), "sql", sql.ptr());
      } else {
        SHARE_SCHEMA_LOG(ERROR, "ai invalidate row done", K(affected), "sql", sql.ptr());
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
  // UPDATE ... WHERE state IN (PENDING, RETRY_WAIT) ... then read-back loop
  // is atomic per statement; concurrent claimants can only lose rows whose
  // state moved. RETRY_WAIT rows become claimable once eligible_ts (set to
  // now() + retry delay by fail_task) passes, which is what makes the
  // bounded-retry loop actually run.
  //
  // The claim identity is the OWNER TOKEN written by the claim UPDATE, not a
  // wall-clock window. The previous read-back filtered on
  // "gmt_modified >= now(6) - INTERVAL 10 SECOND", which made the claim depend
  // on how fast the read ran: any row whose read-back slipped past the window
  // stayed EXECUTING forever (live evidence: 6 rows at state=1, never consumed).
  // A token is exact and cannot expire.
  const int64_t claim_token = ObTimeUtility::current_time();
  ObSqlString claim_sql;
  int64_t affected = 0;
  if (OB_FAIL(claim_sql.assign_fmt(
          "UPDATE %s SET state = %d, claim_owner = '%ld', claim_ts = now(6),"
          " gmt_modified = now(6)"
          " WHERE state IN (%d, %d) AND eligible_ts <= now(6)"
          " ORDER BY task_id ASC LIMIT %ld",
          OB_AI_COLUMN_TASK_TABLE, static_cast<int>(AI_TASK_EXECUTING),
          claim_token,
          static_cast<int>(AI_TASK_PENDING), static_cast<int>(AI_TASK_RETRY_WAIT),
          max_batch))) {
    SHARE_SCHEMA_LOG(ERROR, "build claim sql failed", K(ret));
  } else if (OB_FAIL(exec_write(sql_client, claim_sql, affected))) {
    SHARE_SCHEMA_LOG(ERROR, "claim ai tasks failed", K(ret), K(claim_token));
  } else if (affected > 0) {
    ObSqlString read_sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      // table_id / column_id / task_id are SIGNED bigint inner-table columns,
      // so they must be read with get_int: ObObj::get_uint64() only accepts an
      // exact UInt64 and returns OB_OBJ_TYPE_ERROR (-4001) for a signed int,
      // which is what used to abort this loop on the very first row and leave
      // every claimed task unread (and therefore stuck in EXECUTING).
      if (OB_FAIL(read_sql.assign_fmt(
              "SELECT table_id, task_id, column_id, rowkey_predicate,"
              " request_id, definition_version, retry_count FROM %s"
              " WHERE state = %d AND claim_owner = '%ld'"
              " ORDER BY task_id ASC",
              OB_AI_COLUMN_TASK_TABLE, static_cast<int>(AI_TASK_EXECUTING),
              claim_token))) {
        SHARE_SCHEMA_LOG(ERROR, "build read claimed sql failed", K(ret));
      } else if (OB_FAIL(sql_client.read(res, read_sql.ptr()))) {
        SHARE_SCHEMA_LOG(ERROR, "read claimed tasks failed", K(ret), "sql", read_sql.ptr());
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(ERROR, "claimed tasks result missing", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ClaimedTask task;
          int64_t table_id = 0;
          int64_t task_id = 0;
          int64_t column_id = 0;
          if (OB_FAIL(result->get_int("table_id", table_id))) {
            SHARE_SCHEMA_LOG(ERROR, "read task table id failed", K(ret));
          } else if (OB_FAIL(result->get_int("task_id", task_id))) {
            SHARE_SCHEMA_LOG(ERROR, "read task id failed", K(ret));
          } else if (OB_FAIL(result->get_int("column_id", column_id))) {
            SHARE_SCHEMA_LOG(ERROR, "read task column id failed", K(ret));
          } else if (OB_FAIL(result->get_int("definition_version", task.definition_version_))) {
            SHARE_SCHEMA_LOG(ERROR, "read task definition version failed", K(ret));
          } else if (OB_FAIL(result->get_int("retry_count", task.retry_count_))) {
            SHARE_SCHEMA_LOG(ERROR, "read task retry count failed", K(ret));
          }
          if (OB_FAIL(ret)) {
            // leave loop on read error
          } else {
            ObString tmp;
            ObString rowkey_copy;
            ObString request_copy;
            if (OB_FAIL(result->get_varchar("rowkey_predicate", tmp))) {
              SHARE_SCHEMA_LOG(ERROR, "read rowkey predicate failed", K(ret));
            } else if (OB_FAIL(ob_write_string(allocator_, tmp, rowkey_copy))) {
              SHARE_SCHEMA_LOG(ERROR, "copy rowkey predicate failed", K(ret));
            } else if (OB_FAIL(result->get_varchar("request_id", tmp))) {
              SHARE_SCHEMA_LOG(ERROR, "read request id failed", K(ret));
            } else if (OB_FAIL(ob_write_string(allocator_, tmp, request_copy))) {
              SHARE_SCHEMA_LOG(ERROR, "copy request id failed", K(ret));
            } else {
              task.table_id_ = static_cast<uint64_t>(table_id);
              task.task_id_ = task_id;
              task.column_id_ = static_cast<uint64_t>(column_id);
              task.rowkey_predicate_ = rowkey_copy;
              task.request_id_ = request_copy;
              if (OB_FAIL(tasks.push_back(task))) {
                SHARE_SCHEMA_LOG(ERROR, "append claimed task failed", K(ret));
              }
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    // ERROR level on purpose: WARN is compiled out and these two counts are
    // what localises a claim that marks rows but fails to read them back.
    SHARE_SCHEMA_LOG(ERROR, "ai claim end", K(claim_token), K(affected),
                     "claimed", tasks.count(), K(ret));
    if (OB_SUCC(ret) && tasks.count() != affected) {
      SHARE_SCHEMA_LOG(ERROR, "ai claim count mismatch", K(claim_token),
                       K(affected), "claimed", tasks.count());
    }
  }
  return ret;
}

int ObAiColumnTaskService::recover_stuck_tasks(ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  // Single worker: anything still EXECUTING belongs to a dead claim. Identity
  // is cleared together with the state so the next claim sweep takes the row
  // under a fresh owner token (the old one can never be matched again).
  if (OB_FAIL(sql.assign_fmt(
          "UPDATE %s SET state = %d, claim_owner = '', claim_ts = now(6)"
          " WHERE state = %d",
          OB_AI_COLUMN_TASK_TABLE, static_cast<int>(AI_TASK_PENDING),
          static_cast<int>(AI_TASK_EXECUTING)))) {
    SHARE_SCHEMA_LOG(ERROR, "build recover sql failed", K(ret));
  } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
    SHARE_SCHEMA_LOG(ERROR, "recover stuck tasks failed", K(ret));
  } else if (affected > 0) {
    SHARE_SCHEMA_LOG(ERROR, "recovered stuck ai column tasks", K(affected));
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
      ObSqlString qualified_table;
      ObSqlString sql;
      int64_t affected = 0;
      if (OB_FAIL(get_qualified_table_name(table_schema, qualified_table))) {
        SHARE_SCHEMA_LOG(ERROR, "build qualified table name failed", K(ret),
                         K(table_schema.get_database_id()));
      } else if (OB_FAIL(sql.assign_fmt(
              "UPDATE %.*s SET `%.*s` = %.*s, `%.*s` = 'READY'"
              " WHERE %.*s AND `%.*s` = '%.*s'",
              qualified_table.length(), qualified_table.ptr(),
              result_col->get_column_name_str().length(),
              result_col->get_column_name_str().ptr(),
              entries.at(i).value_.length(), entries.at(i).value_.ptr(),
              state_col->get_column_name_str().length(),
              state_col->get_column_name_str().ptr(),
              rowkey_predicate.length(), rowkey_predicate.ptr(),
              request_col->get_column_name_str().length(),
              request_col->get_column_name_str().ptr(),
              request_id.length(), request_id.ptr()))) {
        SHARE_SCHEMA_LOG(ERROR, "build publish sql failed", K(ret));
      } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
        SHARE_SCHEMA_LOG(ERROR, "publish result failed", K(ret), "sql", sql.ptr());
      } else {
        SHARE_SCHEMA_LOG(ERROR, "ai publish result done", K(affected),
                         K(entries.at(i).column_id_), "sql", sql.ptr());
      }
      // affected == 0 is legitimate: a newer change already took over the
      // row's request id (latest-wins) or the row was deleted (no revive).
    }
  }
  if (OB_SUCC(ret)) {
    // Group is terminal: remove the task rows of THIS publish.
    //
    // The scope is the published column ids, not the whole request: every AI
    // column of one row change shares a single request_id, so a row-wide
    // delete dropped sibling tasks that had not run yet. Their result then
    // never published and the queue drained with NULL AI columns - the exact
    // shape of the round-12 failure (tasks consumed, every summary still NULL).
    ObSqlString col_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
      if (OB_FAIL(col_list.append_fmt("%s%" PRIu64, 0 == i ? "" : ", ",
                                      entries.at(i).column_id_))) {
        SHARE_SCHEMA_LOG(ERROR, "build published column list failed", K(ret));
      }
    }
    ObSqlString del_sql;
    int64_t affected = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(del_sql.assign_fmt(
            "DELETE FROM %s WHERE table_id = %" PRIu64
            " AND rowkey_predicate = '%.*s' AND request_id = '%.*s'"
            " AND column_id IN (%.*s)",
            OB_AI_COLUMN_TASK_TABLE, table_schema.get_table_id(),
            rowkey_predicate.length(), rowkey_predicate.ptr(),
            request_id.length(), request_id.ptr(),
            col_list.length(), col_list.ptr()))) {
      SHARE_SCHEMA_LOG(ERROR, "build publish delete sql failed", K(ret));
    } else if (OB_FAIL(exec_write(sql_client, del_sql, affected))) {
      SHARE_SCHEMA_LOG(ERROR, "remove published tasks failed", K(ret),
                       "sql", del_sql.ptr());
    } else {
      SHARE_SCHEMA_LOG(ERROR, "ai published tasks removed", K(affected),
                       "sql", del_sql.ptr());
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
      ObSqlString qualified_table;
      ObSqlString sql;
      int64_t affected = 0;
      if (OB_FAIL(get_qualified_table_name(table_schema, qualified_table))) {
        SHARE_SCHEMA_LOG(ERROR, "build qualified table name failed", K(ret),
                         K(table_schema.get_database_id()));
      } else if (OB_FAIL(sql.assign_fmt(
              "UPDATE %.*s SET `%.*s` = 'FAILED' WHERE %.*s AND `%.*s` = '%.*s'",
              qualified_table.length(), qualified_table.ptr(),
              state_col->get_column_name_str().length(),
              state_col->get_column_name_str().ptr(),
              rowkey_predicate.length(), rowkey_predicate.ptr(),
              request_col->get_column_name_str().length(),
              request_col->get_column_name_str().ptr(),
              request_id.length(), request_id.ptr()))) {
        SHARE_SCHEMA_LOG(ERROR, "build failure publish sql failed", K(ret));
      } else if (OB_FAIL(exec_write(sql_client, sql, affected))) {
        SHARE_SCHEMA_LOG(ERROR, "publish failure failed", K(ret), "sql", sql.ptr());
      } else {
        SHARE_SCHEMA_LOG(ERROR, "ai publish failure done", K(affected), "sql", sql.ptr());
      }
    }
  }
  if (OB_SUCC(ret)) {
    // Same scoping rule as publish_group: a row change shares one request_id
    // across all AI columns, so a terminal failure of one column must not
    // delete the still-pending task rows of its siblings.
    ObSqlString col_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      if (OB_FAIL(col_list.append_fmt("%s%" PRIu64, 0 == i ? "" : ", ",
                                      tasks.at(i).column_id_))) {
        SHARE_SCHEMA_LOG(ERROR, "build failed column list failed", K(ret));
      }
    }
    ObSqlString del_sql;
    int64_t affected = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(del_sql.assign_fmt(
            "DELETE FROM %s WHERE table_id = %" PRIu64
            " AND rowkey_predicate = '%.*s' AND request_id = '%.*s'"
            " AND column_id IN (%.*s)",
            OB_AI_COLUMN_TASK_TABLE, table_schema.get_table_id(),
            rowkey_predicate.length(), rowkey_predicate.ptr(),
            request_id.length(), request_id.ptr(),
            col_list.length(), col_list.ptr()))) {
      SHARE_SCHEMA_LOG(ERROR, "build failure delete sql failed", K(ret));
    } else if (OB_FAIL(exec_write(sql_client, del_sql, affected))) {
      SHARE_SCHEMA_LOG(ERROR, "remove failed tasks failed", K(ret),
                       "sql", del_sql.ptr());
    } else {
      SHARE_SCHEMA_LOG(ERROR, "ai failed tasks removed", K(affected),
                       "sql", del_sql.ptr());
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
  } else if (extra_predicate.length() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "retry needs a rowkey predicate", K(ret));
  } else {
    // The caller supplies one resolved full rowkey predicate per matched row
    // and its pre-built request id (see ObAiColumnDMLUtils::retry_ai_column).
    // The status pseudo filter is applied by that SELECT, so it is unused here.
    UNUSED(use_status_pseudo);
    UNUSED(status_value);
    UNUSED(request_col);
    UNUSED(result_col);
    UNUSED(state_col);
    if (OB_FAIL(invalidate_row(sql_client, table_schema, extra_predicate,
                               extra_predicate))) {
      SHARE_SCHEMA_LOG(WARN, "retry invalidate row failed", K(ret), "rk", extra_predicate.ptr());
    } else {
      matched_rows = 1;
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
